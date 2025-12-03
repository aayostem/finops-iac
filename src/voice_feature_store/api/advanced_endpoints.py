from fastapi import APIRouter, HTTPException, Depends, Query, BackgroundTasks
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import logging
import asyncio

from ..security.authentication import authenticator, require_scope
from ..ml.training_pipeline import AutoMLTrainer, TrainingConfig
from ..ml.model_serving import ModelMonitoring
from ..quality.validators import FeatureValidator, DataDriftDetector
from ..storage.offline_store import OfflineFeatureStore
from ..storage.online_store import OnlineFeatureStore

logger = logging.getLogger(__name__)

# Create router
advanced_router = APIRouter(prefix="/advanced", tags=["Advanced Features"])

# Global instances
online_store = OnlineFeatureStore()
offline_store = OfflineFeatureStore()
feature_validator = FeatureValidator()
model_monitoring = ModelMonitoring(online_store, offline_store)


@advanced_router.post("/training/train-model")
async def train_model(
    background_tasks: BackgroundTasks,
    model_name: str,
    target_column: str,
    feature_columns: List[str] = Query(
        ..., description="List of feature columns to use"
    ),
    model_type: str = Query(
        "random_forest", description="Model type: random_forest, gradient_boosting, svm"
    ),
    start_date: datetime = Query(..., description="Start date for training data"),
    end_date: datetime = Query(..., description="End date for training data"),
    hyperparameter_tuning: bool = Query(
        True, description="Enable hyperparameter tuning"
    ),
    current_user: Any = Depends(authenticator.require_scope("write:models")),
):
    """Train a new model using historical data."""
    try:
        # Create training configuration
        config = TrainingConfig(
            model_type=model_type,
            target_column=target_column,
            feature_columns=feature_columns,
            hyperparameter_tuning=hyperparameter_tuning,
        )

        # Create trainer
        trainer = AutoMLTrainer(offline_store, model_monitoring.model_registry)

        # Start training in background
        background_tasks.add_task(
            trainer.train_model, model_name, config, start_date, end_date
        )

        return {
            "message": f"Training started for model {model_name}",
            "model_name": model_name,
            "training_period": f"{start_date.date()} to {end_date.date()}",
            "features_used": feature_columns,
            "target_column": target_column,
            "started_at": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.error(f"Training endpoint error: {e}")
        raise HTTPException(status_code=500, detail=f"Training failed: {str(e)}")


@advanced_router.get("/features/quality-report")
async def get_feature_quality_report(
    call_id: Optional[str] = Query(None, description="Specific call ID to analyze"),
    start_date: Optional[datetime] = Query(None, description="Start date for analysis"),
    end_date: Optional[datetime] = Query(None, description="End date for analysis"),
    current_user: Any = Depends(authenticator.require_scope("read:monitoring")),
):
    """Generate comprehensive feature quality report."""
    try:
        quality_report = {
            "summary": {},
            "data_quality": {},
            "temporal_consistency": {},
            "completeness_analysis": {},
            "recommendations": [],
        }

        # Data quality analysis
        if call_id:
            # Single call analysis
            features = online_store.get_latest_features(call_id, "customer")
            if features:
                validation_result = feature_validator.validate_feature_set(
                    features.to_dict()
                )
                quality_report["data_quality"] = {
                    "is_valid": validation_result.is_valid,
                    "errors": validation_result.errors,
                    "warnings": validation_result.warnings,
                    "summary": validation_result.summary,
                }
        else:
            # Bulk analysis
            if start_date and end_date:
                data = offline_store.get_training_data(start_date, end_date)
                if not data.empty:
                    # Analyze data completeness
                    completeness = data.notna().mean().to_dict()
                    quality_report["completeness_analysis"] = {
                        "overall_completeness": data.notna().mean().mean(),
                        "feature_completeness": completeness,
                        "missing_patterns": self._analyze_missing_patterns(data),
                    }

                    # Temporal consistency
                    temporal_data = [row.to_dict() for _, row in data.iterrows()]
                    temporal_result = feature_validator.validate_temporal_consistency(
                        temporal_data
                    )
                    quality_report["temporal_consistency"] = {
                        "is_consistent": temporal_result.is_valid,
                        "warnings": temporal_result.warnings,
                        "summary": temporal_result.summary,
                    }

        # Generate recommendations
        quality_report["recommendations"] = (
            await self._generate_quality_recommendations(quality_report)
        )

        return quality_report

    except Exception as e:
        logger.error(f"Quality report endpoint error: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate quality report")


@advanced_router.get("/features/drift-analysis")
async def get_feature_drift_analysis(
    reference_start: datetime = Query(..., description="Reference period start"),
    reference_end: datetime = Query(..., description="Reference period end"),
    current_start: datetime = Query(..., description="Current period start"),
    current_end: datetime = Query(..., description="Current period end"),
    features: List[str] = Query(None, description="Specific features to analyze"),
    current_user: Any = Depends(authenticator.require_scope("read:monitoring")),
):
    """Analyze feature drift between reference and current periods."""
    try:
        # Load reference data
        reference_data = offline_store.get_training_data(reference_start, reference_end)
        current_data = offline_store.get_training_data(current_start, current_end)

        if reference_data.empty or current_data.empty:
            raise HTTPException(
                status_code=404, detail="Insufficient data for drift analysis"
            )

        # Initialize drift detector
        drift_detector = DataDriftDetector(reference_data)

        # Analyze drift
        drift_report = drift_detector.detect_drift(current_data, threshold=0.1)

        # Calculate overall drift score
        overall_drift = (
            max(drift_report.values(), key=lambda x: x["mean_drift"])["mean_drift"]
            if drift_report
            else 0.0
        )

        # Generate insights
        insights = await self._generate_drift_insights(drift_report, overall_drift)

        return {
            "analysis_period": {
                "reference": f"{reference_start.date()} to {reference_end.date()}",
                "current": f"{current_start.date()} to {current_end.date()}",
            },
            "overall_drift_score": overall_drift,
            "drift_status": (
                "high"
                if overall_drift > 0.2
                else "medium" if overall_drift > 0.1 else "low"
            ),
            "feature_drift": drift_report,
            "insights": insights,
            "recommendations": self._generate_drift_recommendations(
                drift_report, overall_drift
            ),
        }

    except Exception as e:
        logger.error(f"Drift analysis endpoint error: {e}")
        raise HTTPException(status_code=500, detail="Failed to perform drift analysis")


@advanced_router.get("/features/statistics")
async def get_feature_statistics(
    start_date: datetime = Query(..., description="Start date for statistics"),
    end_date: datetime = Query(..., description="End date for statistics"),
    feature_names: List[str] = Query(None, description="Specific features to analyze"),
    current_user: Any = Depends(authenticator.require_scope("read:features")),
):
    """Get comprehensive statistics for features."""
    try:
        data = offline_store.get_training_data(start_date, end_date)

        if data.empty:
            return {
                "message": "No data available for the specified period",
                "statistics": {},
            }

        # Filter features if specified
        if feature_names:
            available_features = [f for f in feature_names if f in data.columns]
            data = data[available_features]
        else:
            # Use numeric features by default
            numeric_features = data.select_dtypes(include=[np.number]).columns.tolist()
            data = data[numeric_features]

        statistics = {}
        for column in data.columns:
            column_data = data[column].dropna()
            if len(column_data) > 0:
                statistics[column] = {
                    "count": len(column_data),
                    "mean": float(column_data.mean()),
                    "std": float(column_data.std()),
                    "min": float(column_data.min()),
                    "max": float(column_data.max()),
                    "median": float(column_data.median()),
                    "q1": float(column_data.quantile(0.25)),
                    "q3": float(column_data.quantile(0.75)),
                    "missing_count": data[column].isna().sum(),
                    "missing_percentage": data[column].isna().mean(),
                    "outliers": self._detect_outliers(column_data),
                }

        return {
            "period": f"{start_date.date()} to {end_date.date()}",
            "total_samples": len(data),
            "features_analyzed": list(statistics.keys()),
            "statistics": statistics,
        }

    except Exception as e:
        logger.error(f"Feature statistics endpoint error: {e}")
        raise HTTPException(
            status_code=500, detail="Failed to compute feature statistics"
        )


@advanced_router.post("/features/backfill")
async def backfill_features(
    background_tasks: BackgroundTasks,
    call_ids: List[str] = Query(..., description="List of call IDs to backfill"),
    start_date: datetime = Query(..., description="Start date for backfill"),
    end_date: datetime = Query(..., description="End date for backfill"),
    reprocess_audio: bool = Query(False, description="Reprocess audio from source"),
    current_user: Any = Depends(authenticator.require_scope("write:features")),
):
    """Backfill features for historical calls."""
    try:
        # This would trigger a backfill process in production
        # For now, simulate the action

        background_tasks.add_task(
            self._execute_backfill, call_ids, start_date, end_date, reprocess_audio
        )

        return {
            "message": f"Backfill started for {len(call_ids)} calls",
            "call_ids": call_ids,
            "period": f"{start_date.date()} to {end_date.date()}",
            "reprocess_audio": reprocess_audio,
            "started_at": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.error(f"Backfill endpoint error: {e}")
        raise HTTPException(status_code=500, detail="Backfill failed")


# Helper methods
async def _generate_quality_recommendations(self, quality_report: Dict) -> List[str]:
    """Generate quality improvement recommendations."""
    recommendations = []

    completeness = quality_report.get("completeness_analysis", {})
    if completeness.get("overall_completeness", 1.0) < 0.9:
        recommendations.append(
            "Consider implementing feature imputation for missing values"
        )

    temporal_consistency = quality_report.get("temporal_consistency", {})
    if not temporal_consistency.get("is_consistent", True):
        recommendations.append(
            "Review feature computation for temporal consistency issues"
        )

    data_quality = quality_report.get("data_quality", {})
    if data_quality.get("errors"):
        recommendations.append("Address data validation errors in feature computation")

    if not recommendations:
        recommendations.append("Data quality is good. Continue current practices.")

    return recommendations


async def _generate_drift_insights(
    self, drift_report: Dict, overall_drift: float
) -> List[str]:
    """Generate insights from drift analysis."""
    insights = []

    if overall_drift > 0.2:
        insights.append(
            "Significant feature drift detected. Model retraining recommended."
        )

    high_drift_features = [
        feature
        for feature, report in drift_report.items()
        if report["mean_drift"] > 0.15
    ]

    if high_drift_features:
        insights.append(f"High drift in features: {', '.join(high_drift_features[:3])}")

    # Analyze drift patterns
    positive_drift = [f for f, r in drift_report.items() if r["mean_drift"] > 0]
    negative_drift = [f for f, r in drift_report.items() if r["mean_drift"] < 0]

    if len(positive_drift) > len(negative_drift):
        insights.append(
            "Overall feature values are increasing compared to reference period"
        )
    else:
        insights.append(
            "Overall feature values are decreasing compared to reference period"
        )

    return insights


def _generate_drift_recommendations(
    self, drift_report: Dict, overall_drift: float
) -> List[str]:
    """Generate recommendations based on drift analysis."""
    recommendations = []

    if overall_drift > 0.2:
        recommendations.append("Immediate model retraining required")
    elif overall_drift > 0.1:
        recommendations.append("Schedule model retraining in near future")

    # Feature-specific recommendations
    for feature, report in drift_report.items():
        if report["mean_drift"] > 0.15:
            recommendations.append(f"Investigate root cause for drift in {feature}")

    if overall_drift < 0.05:
        recommendations.append("Feature stability is good. Continue monitoring.")

    return recommendations


def _analyze_missing_patterns(self, data: pd.DataFrame) -> Dict:
    """Analyze patterns in missing data."""
    missing_matrix = data.isna()

    return {
        "total_missing": missing_matrix.sum().sum(),
        "missing_by_feature": missing_matrix.sum().to_dict(),
        "missing_by_sample": missing_matrix.sum(axis=1).value_counts().to_dict(),
        "correlation_analysis": "Advanced missing pattern analysis would go here",
    }


def _detect_outliers(self, data: pd.Series) -> Dict:
    """Detect outliers in feature data."""
    Q1 = data.quantile(0.25)
    Q3 = data.quantile(0.75)
    IQR = Q3 - Q1

    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    outliers = data[(data < lower_bound) | (data > upper_bound)]

    return {
        "count": len(outliers),
        "percentage": len(outliers) / len(data),
        "lower_bound": float(lower_bound),
        "upper_bound": float(upper_bound),
    }


async def _execute_backfill(
    self,
    call_ids: List[str],
    start_date: datetime,
    end_date: datetime,
    reprocess_audio: bool,
):
    """Execute backfill process (simulated)."""
    logger.info(f"Starting backfill for {len(call_ids)} calls")

    # Simulate processing
    for i, call_id in enumerate(call_ids):
        if i % 100 == 0:
            logger.info(f"Backfill progress: {i}/{len(call_ids)}")
        await asyncio.sleep(0.01)  # Simulate work

    logger.info("Backfill completed")
