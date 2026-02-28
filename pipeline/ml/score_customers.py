from __future__ import annotations

import argparse
import shutil
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from pipeline.common.config import settings
from pipeline.ml.scoring_common import ModelScorer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate customer credit scores from offline Gold features.")
    parser.add_argument(
        "--features-path",
        default=str(settings.gold_dir / "customer_credit_features"),
        help="Path to gold.customer_credit_features parquet",
    )
    parser.add_argument(
        "--model-path",
        default=str(settings.artifacts_dir / "lightgbm_credit_model.pkl"),
        help="Path to serialized LightGBM model",
    )
    parser.add_argument(
        "--metadata-path",
        default=str(settings.artifacts_dir / "model_metadata.json"),
        help="Path to model metadata json",
    )
    parser.add_argument(
        "--output-path",
        default=str(settings.gold_dir / "customer_credit_scores"),
        help="Output path for gold.customer_credit_scores parquet",
    )
    parser.add_argument("--batch-size", type=int, default=50000)
    return parser.parse_args()


def _path(value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else Path.cwd() / value


def main() -> None:
    args = parse_args()
    features_path = _path(args.features_path)
    output_path = _path(args.output_path)
    scorer = ModelScorer.load(model_path=args.model_path, metadata_path=args.metadata_path)

    dataset = ds.dataset(str(features_path), format="parquet")
    feature_columns = scorer.feature_columns
    read_columns = ["customer_id", "institution_id", "source_system", *feature_columns]

    if output_path.exists():
        shutil.rmtree(output_path)
    output_path.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    for batch_index, batch in enumerate(dataset.to_batches(columns=read_columns, batch_size=args.batch_size)):
        rows = batch.to_pylist()
        feature_rows = [{column: row.get(column) for column in feature_columns} for row in rows]
        scored_rows = scorer.score_rows(feature_rows)
        score_generated_at = datetime.now(UTC).replace(tzinfo=None)

        output_rows = []
        for row, scored in zip(rows, scored_rows, strict=True):
            output_rows.append(
                {
                    "customer_id": int(row["customer_id"]),
                    "institution_id": row.get("institution_id"),
                    "source_system": row.get("source_system"),
                    "credit_score": int(scored["credit_score"]),
                    "default_probability": float(scored["default_probability"]),
                    "risk_level": str(scored["risk_level"]),
                    "score_generated_at": score_generated_at,
                }
            )

        table = pa.Table.from_pylist(output_rows)
        pq.write_table(table, output_path / f"part-{batch_index:05d}.parquet")
        total_rows += len(output_rows)

    print(f"Scores saved to {output_path}")
    print(f"Rows scored: {total_rows}")


if __name__ == "__main__":
    main()
