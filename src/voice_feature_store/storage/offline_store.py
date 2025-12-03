import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import logging
import tempfile
import os
from ..features.voice_features import VoiceFeatures
from config.settings import settings


logger = logging.getLogger(__name__)


class OfflineFeatureStore:
    """S3-based offline feature store for historical data and training."""

    def __init__(self):
        self.s3_client = self._create_s3_client()
        self.bucket = settings.s3_bucket
        self.base_path = settings.s3_offline_path

        # Define schema for consistent storage
        self.schema = pa.schema(
            [
                pa.field("call_id", pa.string()),
                pa.field("speaker_id", pa.string()),
                pa.field("timestamp", pa.timestamp("ms")),
                pa.field("window_duration", pa.float64()),
                pa.field("talk_to_listen_ratio", pa.float64()),
                pa.field("interruption_count", pa.int64()),
                pa.field("silence_ratio", pa.float64()),
                pa.field("speaking_duration", pa.float64()),
                pa.field("avg_pitch", pa.float64()),
                pa.field("pitch_variance", pa.float64()),
                pa.field("energy_db", pa.float64()),
                pa.field("spectral_centroid", pa.float64()),
                pa.field("mfccs", pa.list_(pa.float64())),
                pa.field("voice_activity", pa.bool_()),
                pa.field("voice_confidence", pa.float64()),
                pa.field("ingestion_timestamp", pa.timestamp("ms")),
            ]
        )

    def _create_s3_client(self):
        """Create S3 client with credentials."""
        return boto3.client(
            "s3",
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region,
        )

    def _get_s3_path(self, date: datetime) -> str:
        """Generate S3 path with partitioning by date."""
        return f"{self.base_path}date={date.strftime('%Y-%m-%d')}/hour={date.strftime('%H')}/data.parquet"

    def store_features_batch(self, features_list: List[VoiceFeatures]) -> bool:
        """Store a batch of features in S3 as Parquet."""
        if not features_list:
            return True

        try:
            # Convert to pandas DataFrame
            data = []
            for features in features_list:
                feature_dict = features.to_dict()
                feature_dict["ingestion_timestamp"] = datetime.utcnow()
                feature_dict["mfccs"] = feature_dict["mfccs"] or []
                data.append(feature_dict)

            df = pd.DataFrame(data)

            # Convert timestamp strings to datetime
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df["ingestion_timestamp"] = pd.to_datetime(df["ingestion_timestamp"])

            # Use the first feature's timestamp for partitioning
            partition_date = features_list[0].timestamp

            # Convert to PyArrow Table
            table = pa.Table.from_pandas(df, schema=self.schema)

            # Write to temporary file and upload to S3
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
                try:
                    pq.write_table(table, f.name, compression="snappy")

                    s3_key = self._get_s3_path(partition_date)
                    self.s3_client.upload_file(f.name, self.bucket, s3_key)

                    logger.info(
                        f"Stored {len(features_list)} features to s3://{self.bucket}/{s3_key}"
                    )
                    return True

                finally:
                    os.unlink(f.name)  # Clean up temp file

        except Exception as e:
            logger.error(f"Failed to store features batch: {e}")
            return False

    def get_training_data(
        self,
        start_date: datetime,
        end_date: datetime,
        call_ids: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Retrieve historical features for model training."""
        try:
            # Generate date range for S3 paths
            date_range = pd.date_range(start_date, end_date, freq="D")
            all_tables = []

            for date in date_range:
                s3_prefix = f"{self.base_path}date={date.strftime('%Y-%m-%d')}/"

                # List all Parquet files for this date
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket, Prefix=s3_prefix
                )

                if "Contents" not in response:
                    continue

                for obj in response["Contents"]:
                    if obj["Key"].endswith(".parquet"):
                        # Download and read Parquet file
                        with tempfile.NamedTemporaryFile(suffix=".parquet") as f:
                            self.s3_client.download_file(
                                self.bucket, obj["Key"], f.name
                            )
                            table = pq.read_table(f.name)
                            all_tables.append(table)

            if not all_tables:
                return pd.DataFrame()

            # Combine all tables
            combined_table = pa.concat_tables(all_tables)
            df = combined_table.to_pandas()

            # Apply filters
            mask = (df["timestamp"] >= start_date) & (df["timestamp"] <= end_date)
            if call_ids:
                mask &= df["call_id"].isin(call_ids)

            return df[mask].reset_index(drop=True)

        except Exception as e:
            logger.error(f"Failed to retrieve training data: {e}")
            return pd.DataFrame()

    def create_point_in_time_correct_dataset(
        self,
        prediction_times: List[datetime],
        feature_lookback: timedelta = timedelta(hours=1),
    ) -> pd.DataFrame:
        """Create point-in-time correct dataset for training to avoid data leakage."""
        try:
            all_features = []

            for pred_time in prediction_times:
                start_time = pred_time - feature_lookback

                # Get features from lookback window
                features_df = self.get_training_data(start_time, pred_time)

                if not features_df.empty:
                    # Aggregate features per call/speaker for the lookback window
                    aggregated = (
                        features_df.groupby(["call_id", "speaker_id"])
                        .agg(
                            {
                                "talk_to_listen_ratio": "mean",
                                "interruption_count": "sum",
                                "silence_ratio": "mean",
                                "avg_pitch": "mean",
                                "pitch_variance": "mean",
                                "energy_db": "mean",
                                "voice_confidence": "mean",
                                "timestamp": "max",  # Latest timestamp
                            }
                        )
                        .reset_index()
                    )

                    aggregated["prediction_time"] = pred_time
                    all_features.append(aggregated)

            return (
                pd.concat(all_features, ignore_index=True)
                if all_features
                else pd.DataFrame()
            )

        except Exception as e:
            logger.error(f"Failed to create point-in-time dataset: {e}")
            return pd.DataFrame()
