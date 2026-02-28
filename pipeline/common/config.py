import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    source_system: str = os.getenv("SOURCE_SYSTEM", "home_credit")
    institution_id: str = os.getenv("INSTITUTION_ID", "home_credit")
    raw_data_dir: Path = Path(os.getenv("RAW_DATA_DIR", "data/home-credit-default-risk"))
    medallion_root: Path = Path(os.getenv("MEDALLION_ROOT", "data/medallion"))
    artifacts_dir: Path = Path(os.getenv("ARTIFACTS_DIR", "artifacts"))

    @property
    def bronze_dir(self) -> Path:
        return self.medallion_root / "bronze"

    @property
    def silver_dir(self) -> Path:
        return self.medallion_root / "silver"

    @property
    def gold_dir(self) -> Path:
        return self.medallion_root / "gold"


settings = Settings()
