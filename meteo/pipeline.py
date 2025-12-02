# meteo/pipeline.py
from pathlib import Path
import pandas as pd

from .cleaners import DailyCleaner, HourlyCleaner


class MeteoPipeline:
    def __init__(self, data_dir: str | Path, output_dir: str | Path | None = None):
        self.data_dir = Path(data_dir)
        self.output_dir = Path(output_dir) if output_dir else self.data_dir / "clean"
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.daily_cleaner = DailyCleaner()
        self.hourly_cleaner = HourlyCleaner()

    def _read_csv(self, filename: str) -> pd.DataFrame:
        path = self.data_dir / filename
        return pd.read_csv(path)

    def _save_csv(self, df: pd.DataFrame, filename: str) -> Path:
        path = self.output_dir / filename
        df.to_csv(path, index=False)
        return path

    def run(self) -> dict:
        """
        Exécute tout le pipeline :
        - daily_history
        - daily_forecast
        - hourly_history
        - hourly_forecast
        Retourne les DataFrames nettoyés.
        """
        # 1. Chargement
        daily_history_raw = self._read_csv("meteo_daily_history_2023-01-01_2025-12-02.csv")
        daily_forecast_raw = self._read_csv("meteo_daily_forecast_2025-12-03.csv")
        hourly_history_raw = self._read_csv("meteo_hourly_history_2023-01-01_2025-12-02.csv")
        hourly_forecast_raw = self._read_csv("meteo_hourly_forecast_2025-12-03.csv")

        # 2. Nettoyage
        daily_history_clean = self.daily_cleaner.clean(daily_history_raw)
        daily_forecast_clean = self.daily_cleaner.clean(daily_forecast_raw)

        hourly_history_clean = self.hourly_cleaner.clean(hourly_history_raw)
        hourly_forecast_clean = self.hourly_cleaner.clean(hourly_forecast_raw)

        # 3. Sauvegarde
        self._save_csv(daily_history_clean, "daily_history_clean.csv")
        self._save_csv(daily_forecast_clean, "daily_forecast_clean.csv")
        self._save_csv(hourly_history_clean, "hourly_history_clean.csv")
        self._save_csv(hourly_forecast_clean, "hourly_forecast_clean.csv")

        return {
            "daily_history": daily_history_clean,
            "daily_forecast": daily_forecast_clean,
            "hourly_history": hourly_history_clean,
            "hourly_forecast": hourly_forecast_clean,
        }
