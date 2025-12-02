# meteo/pipeline.py
from pathlib import Path
import pandas as pd

# Imports
from cleaners import HourlyCleaner
from meteo import MeteoFetcher 

class MeteoPipeline:
    def __init__(self, base_dir: str = "data"):
        self.base_dir = Path(base_dir)
        self.raw_dir = self.base_dir / "raw"
        self.clean_dir = self.base_dir / "clean"
        self.clean_dir.mkdir(parents=True, exist_ok=True)

        # Instanciation
        self.fetcher = MeteoFetcher(raw_dir=self.raw_dir)
        self.hourly_cleaner = HourlyCleaner()

    def _save_clean(self, df: pd.DataFrame, filename: str) -> Path:
        path = self.clean_dir / filename
        df.to_csv(path, index=False)
        print(f"   ✨ Données propres sauvegardées : {path}")
        return path

    def run(self) -> dict:
        print("\n=== EXÉCUTION DU PIPELINE MÉTÉO (HORAIRE) ===")
        
        # 1. EXTRACTION
        self.fetcher.download_all()

        # 2. CHARGEMENT
        print("\n--- 2. Chargement des données brutes ---")
        try:
            hourly_hist = pd.read_csv(self.raw_dir / "raw_hourly_history.csv")
            hourly_fore = pd.read_csv(self.raw_dir / "raw_hourly_forecast.csv")
        except FileNotFoundError as e:
            print(f"❌ Erreur critique : Fichier manquant. {e}")
            return {}

        # 3. TRANSFORMATION
        print("\n--- 3. Nettoyage ---")
        hourly_hist_clean = self.hourly_cleaner.clean(hourly_hist)
        hourly_fore_clean = self.hourly_cleaner.clean(hourly_fore)

        # 4. SAUVEGARDE
        print("\n--- 4. Export ---")
        res = {
            "hourly_history": self._save_clean(hourly_hist_clean, "hourly_history.csv"),
            "hourly_forecast": self._save_clean(hourly_fore_clean, "hourly_forecast.csv"),
        }
        
        print("\n✅ Pipeline terminé.")
        return res