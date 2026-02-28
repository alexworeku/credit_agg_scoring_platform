import argparse
import json
import pickle
from pathlib import Path

import lightgbm as lgb
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, roc_auc_score
from sklearn.model_selection import train_test_split

from pipeline.common.config import settings
from pipeline.common.spark_utils import build_spark
from pipeline.ml.scoring_common import FEATURE_COLUMNS


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train LightGBM credit default model")
    parser.add_argument(
        "--features-path",
        default=str(settings.gold_dir / "customer_credit_features"),
        help="Path to gold.customer_credit_features parquet",
    )
    parser.add_argument(
        "--model-path",
        default=str(settings.artifacts_dir / "lightgbm_credit_model.pkl"),
        help="Output path for serialized model",
    )
    parser.add_argument(
        "--metadata-path",
        default=str(settings.artifacts_dir / "model_metadata.json"),
        help="Output path for training metadata",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = build_spark("credit-agg-train-model")
    features_df = spark.read.parquet(args.features_path)

    selected = features_df.select(*FEATURE_COLUMNS, "default_flag").fillna(0.0)
    rows = selected.collect()

    if not rows:
        spark.stop()
        raise ValueError("No rows found in customer_credit_features. Run feature engineering first.")

    x = np.array([[float(row[c]) for c in FEATURE_COLUMNS] for row in rows], dtype=np.float64)
    y = np.array([int(row["default_flag"]) for row in rows], dtype=np.int32)

    unique_classes = np.unique(y)
    if unique_classes.shape[0] < 2:
        spark.stop()
        raise ValueError("Training target has fewer than 2 classes. Cannot train a classifier.")

    x_train, x_valid, y_train, y_valid = train_test_split(
        x,
        y,
        test_size=0.2,
        random_state=42,
        stratify=y,
    )

    model = lgb.LGBMClassifier(
        n_estimators=500,
        learning_rate=0.03,
        num_leaves=31,
        colsample_bytree=0.8,
        subsample=0.8,
        random_state=42,
        objective="binary",
        class_weight="balanced",
    )
    model.fit(x_train, y_train)

    valid_proba = model.predict_proba(x_valid)[:, 1]
    clipped_valid_proba = np.clip(valid_proba, 1e-6, 1.0 - 1e-6)
    valid_logit = np.log(clipped_valid_proba / (1.0 - clipped_valid_proba)).reshape(-1, 1)
    calibrator = LogisticRegression(max_iter=1000, solver="lbfgs")
    calibrator.fit(valid_logit, y_valid)
    calibrated_valid_proba = calibrator.predict_proba(valid_logit)[:, 1]
    auc = float(roc_auc_score(y_valid, valid_proba))
    raw_brier = float(brier_score_loss(y_valid, clipped_valid_proba))
    calibrated_brier = float(brier_score_loss(y_valid, calibrated_valid_proba))
    base_default_rate = float(np.mean(y))

    model_path = Path(args.model_path)
    model_path.parent.mkdir(parents=True, exist_ok=True)
    with model_path.open("wb") as f:
        pickle.dump(model, f)

    metadata = {
        "feature_columns": FEATURE_COLUMNS,
        "auc": auc,
        "train_rows": int(x_train.shape[0]),
        "valid_rows": int(x_valid.shape[0]),
        "base_default_rate": base_default_rate,
        "probability_calibration": {
            "method": "platt_logit",
            "intercept": float(calibrator.intercept_[0]),
            "coef": float(calibrator.coef_[0][0]),
            "valid_brier_raw": raw_brier,
            "valid_brier_calibrated": calibrated_brier,
        },
        "scorecard": {
            "method": "log_odds",
            "base_score": 600.0,
            "base_odds": float((1.0 - base_default_rate) / max(base_default_rate, 1e-6)),
            "pdo": 20.0,
            "min_score": 300,
            "max_score": 850,
        },
        "risk_bands": {
            "low_max_default_probability": 0.05,
            "medium_max_default_probability": 0.15,
        },
    }
    metadata_path = Path(args.metadata_path)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    spark.stop()
    print(f"Training complete. Validation AUC: {auc:.6f}")
    print(f"Validation Brier (raw): {raw_brier:.6f}")
    print(f"Validation Brier (calibrated): {calibrated_brier:.6f}")
    print(f"Model saved to {model_path}")
    print(f"Metadata saved to {metadata_path}")


if __name__ == "__main__":
    main()
