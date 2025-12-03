import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass
import pickle
import json
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.svm import SVC
from sklearn.metrics import classification_report, accuracy_score, f1_score
from sklearn.preprocessing import StandardScaler
import optuna
from prometheus_client import Counter, Histogram, Gauge

logger = logging.getLogger(__name__)

# Training pipeline metrics
MODEL_TRAINING_COUNT = Counter(
    "model_training_total", "Total model training runs", ["model_type", "status"]
)
MODEL_TRAINING_DURATION = Histogram(
    "model_training_duration_seconds", "Model training duration", ["model_type"]
)
MODEL_PERFORMANCE = Gauge(
    "model_performance_metrics", "Model performance metrics", ["model_name", "metric"]
)


@dataclass
class TrainingConfig:
    """Configuration for model training."""

    model_type: str  # 'random_forest', 'gradient_boosting', 'svm'
    target_column: str
    feature_columns: List[str]
    test_size: float = 0.2
    random_state: int = 42
    cv_folds: int = 5
    hyperparameter_tuning: bool = True
    n_trials: int = 100


@dataclass
class TrainingResult:
    """Result of model training."""

    model_name: str
    model_version: str
    model: Any
    training_config: TrainingConfig
    performance_metrics: Dict[str, float]
    feature_importance: Dict[str, float]
    training_data_info: Dict[str, Any]
    timestamp: datetime
    cross_val_scores: List[float]


class AutoMLTrainer:
    """Automated machine learning for voice feature models."""

    def __init__(self, offline_store, model_registry):
        self.offline_store = offline_store
        self.model_registry = model_registry
        self.scaler = StandardScaler()

    async def train_model(
        self,
        model_name: str,
        config: TrainingConfig,
        start_date: datetime,
        end_date: datetime,
    ) -> TrainingResult:
        """Train a new model version using historical data."""
        training_start = datetime.utcnow()

        try:
            logger.info(
                f"Starting training for {model_name} with data from {start_date} to {end_date}"
            )

            # 1. Load training data
            training_data = await self._load_training_data(config, start_date, end_date)
            if training_data.empty:
                raise ValueError("No training data available for the specified period")

            # 2. Preprocess data
            X_train, X_test, y_train, y_test = await self._preprocess_data(
                training_data, config
            )

            # 3. Hyperparameter tuning
            best_params = await self._optimize_hyperparameters(config, X_train, y_train)

            # 4. Train final model
            model = await self._train_final_model(config, best_params, X_train, y_train)

            # 5. Evaluate model
            performance_metrics, feature_importance = await self._evaluate_model(
                model, X_test, y_test, config
            )

            # 6. Cross-validation
            cv_scores = await self._cross_validate(model, X_train, y_train, config)

            # 7. Create training result
            training_result = TrainingResult(
                model_name=model_name,
                model_version=f"v{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
                model=model,
                training_config=config,
                performance_metrics=performance_metrics,
                feature_importance=feature_importance,
                training_data_info={
                    "samples": len(training_data),
                    "features": len(config.feature_columns),
                    "period": f"{start_date.date()} to {end_date.date()}",
                },
                timestamp=datetime.utcnow(),
                cross_val_scores=cv_scores,
            )

            # 8. Register model
            await self._register_model(training_result)

            # Record metrics
            training_duration = (datetime.utcnow() - training_start).total_seconds()
            MODEL_TRAINING_DURATION.labels(model_type=config.model_type).observe(
                training_duration
            )
            MODEL_TRAINING_COUNT.labels(
                model_type=config.model_type, status="success"
            ).inc()

            # Update performance metrics
            for metric, value in performance_metrics.items():
                MODEL_PERFORMANCE.labels(model_name=model_name, metric=metric).set(
                    value
                )

            logger.info(
                f"Training completed for {model_name}: accuracy={performance_metrics['accuracy']:.3f}"
            )
            return training_result

        except Exception as e:
            logger.error(f"Training failed for {model_name}: {e}")
            MODEL_TRAINING_COUNT.labels(
                model_type=config.model_type, status="error"
            ).inc()
            raise

    async def _load_training_data(
        self, config: TrainingConfig, start_date: datetime, end_date: datetime
    ) -> pd.DataFrame:
        """Load and prepare training data."""
        # Load features from offline store
        features_df = self.offline_store.get_training_data(start_date, end_date)

        if features_df.empty:
            return pd.DataFrame()

        # Ensure required columns exist
        required_columns = config.feature_columns + [config.target_column]
        missing_columns = [
            col for col in required_columns if col not in features_df.columns
        ]

        if missing_columns:
            logger.warning(f"Missing columns in training data: {missing_columns}")
            # Drop rows with missing target
            features_df = features_df.dropna(subset=[config.target_column])

        return features_df

    async def _preprocess_data(
        self, data: pd.DataFrame, config: TrainingConfig
    ) -> Tuple:
        """Preprocess training data."""
        # Handle missing values
        data = data.dropna(subset=config.feature_columns + [config.target_column])

        # Extract features and target
        X = data[config.feature_columns]
        y = data[config.target_column]

        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        X_scaled = pd.DataFrame(X_scaled, columns=config.feature_columns, index=X.index)

        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X_scaled,
            y,
            test_size=config.test_size,
            random_state=config.random_state,
            stratify=y,
        )

        return X_train, X_test, y_train, y_test

    async def _optimize_hyperparameters(
        self, config: TrainingConfig, X: pd.DataFrame, y: pd.Series
    ) -> Dict[str, Any]:
        """Optimize hyperparameters using Optuna."""
        if not config.hyperparameter_tuning:
            return self._get_default_parameters(config.model_type)

        def objective(trial):
            if config.model_type == "random_forest":
                params = {
                    "n_estimators": trial.suggest_int("n_estimators", 50, 500),
                    "max_depth": trial.suggest_int("max_depth", 3, 20),
                    "min_samples_split": trial.suggest_int("min_samples_split", 2, 20),
                    "min_samples_leaf": trial.suggest_int("min_samples_leaf", 1, 10),
                    "max_features": trial.suggest_categorical(
                        "max_features", ["sqrt", "log2"]
                    ),
                }
                model = RandomForestClassifier(
                    **params, random_state=config.random_state
                )

            elif config.model_type == "gradient_boosting":
                params = {
                    "n_estimators": trial.suggest_int("n_estimators", 50, 500),
                    "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3),
                    "max_depth": trial.suggest_int("max_depth", 3, 10),
                    "min_samples_split": trial.suggest_int("min_samples_split", 2, 20),
                    "min_samples_leaf": trial.suggest_int("min_samples_leaf", 1, 10),
                    "subsample": trial.suggest_float("subsample", 0.6, 1.0),
                }
                model = GradientBoostingClassifier(
                    **params, random_state=config.random_state
                )

            elif config.model_type == "svm":
                params = {
                    "C": trial.suggest_float("C", 0.1, 10.0),
                    "kernel": trial.suggest_categorical(
                        "kernel", ["linear", "rbf", "poly"]
                    ),
                    "gamma": trial.suggest_categorical("gamma", ["scale", "auto"]),
                }
                model = SVC(
                    **params, probability=True, random_state=config.random_state
                )

            else:
                raise ValueError(f"Unsupported model type: {config.model_type}")

            # Use cross-validation score
            score = cross_val_score(
                model, X, y, cv=config.cv_folds, scoring="f1_weighted"
            ).mean()
            return score

        study = optuna.create_study(direction="maximize")
        study.optimize(objective, n_trials=config.n_trials)

        return study.best_params

    async def _train_final_model(
        self, config: TrainingConfig, best_params: Dict, X: pd.DataFrame, y: pd.Series
    ) -> Any:
        """Train the final model with best parameters."""
        if config.model_type == "random_forest":
            model = RandomForestClassifier(
                **best_params, random_state=config.random_state
            )
        elif config.model_type == "gradient_boosting":
            model = GradientBoostingClassifier(
                **best_params, random_state=config.random_state
            )
        elif config.model_type == "svm":
            model = SVC(
                **best_params, probability=True, random_state=config.random_state
            )
        else:
            raise ValueError(f"Unsupported model type: {config.model_type}")

        model.fit(X, y)
        return model

    async def _evaluate_model(
        self,
        model: Any,
        X_test: pd.DataFrame,
        y_test: pd.Series,
        config: TrainingConfig,
    ) -> Tuple[Dict, Dict]:
        """Evaluate model performance."""
        # Predictions
        y_pred = model.predict(X_test)
        y_pred_proba = model.predict_proba(X_test)

        # Calculate metrics
        accuracy = accuracy_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred, average="weighted")

        # Feature importance
        feature_importance = {}
        if hasattr(model, "feature_importances_"):
            importance_scores = model.feature_importances_
            feature_importance = {
                feature: score
                for feature, score in zip(config.feature_columns, importance_scores)
            }

        metrics = {
            "accuracy": accuracy,
            "f1_score": f1,
            "test_samples": len(X_test),
            "feature_count": len(config.feature_columns),
        }

        # Add class-wise metrics for classification report
        class_report = classification_report(y_test, y_pred, output_dict=True)
        for class_name, class_metrics in class_report.items():
            if (
                class_name not in ["accuracy", "macro avg", "weighted avg"]
                and not class_name.isdigit()
            ):
                continue
            for metric_name, metric_value in class_metrics.items():
                metrics[f"{class_name}_{metric_name}"] = metric_value

        return metrics, feature_importance

    async def _cross_validate(
        self, model: Any, X: pd.DataFrame, y: pd.Series, config: TrainingConfig
    ) -> List[float]:
        """Perform cross-validation."""
        cv_scores = cross_val_score(
            model, X, y, cv=config.cv_folds, scoring="f1_weighted"
        )
        return cv_scores.tolist()

    async def _register_model(self, training_result: TrainingResult):
        """Register the trained model in the model registry."""
        model_info = {
            "path": f"models/{training_result.model_name}/{training_result.model_version}/model.pkl",
            "created_at": training_result.timestamp.isoformat(),
            "metrics": training_result.performance_metrics,
            "feature_set": training_result.training_config.feature_columns,
            "target_latency_ms": 50,
            "training_data_info": training_result.training_data_info,
            "feature_importance": training_result.feature_importance,
            "cross_val_scores": training_result.cross_val_scores,
        }

        # Save model to storage
        await self._save_model(training_result.model, model_info["path"])

        # Register in model registry
        self.model_registry.register_model(
            training_result.model_name, training_result.model_version, model_info
        )

    async def _save_model(self, model: Any, model_path: str):
        """Save model to persistent storage."""
        # In production, this would save to S3 or model registry
        # For now, just log the action
        logger.info(f"Model saved to {model_path}")

    def _get_default_parameters(self, model_type: str) -> Dict[str, Any]:
        """Get default parameters for each model type."""
        defaults = {
            "random_forest": {
                "n_estimators": 100,
                "max_depth": 10,
                "min_samples_split": 2,
                "min_samples_leaf": 1,
                "max_features": "sqrt",
            },
            "gradient_boosting": {
                "n_estimators": 100,
                "learning_rate": 0.1,
                "max_depth": 5,
                "min_samples_split": 2,
                "min_samples_leaf": 1,
                "subsample": 0.8,
            },
            "svm": {"C": 1.0, "kernel": "rbf", "gamma": "scale"},
        }
        return defaults.get(model_type, {})
