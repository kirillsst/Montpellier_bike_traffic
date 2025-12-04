# meteo/pipeline.py
from pathlib import Path
import pandas as pd

# Imports
from data_meteo.cleaners import HourlyCleaner
from data_meteo.meteo import MeteoFetcher
from data_meteo.supabase_client import supabase 

class MeteoPipeline:
    def __init__(self, base_dir: str = "data"):
        self.base_dir = Path(base_dir)
        self.raw_dir = self.base_dir / "raw"
        self.raw_dir.mkdir(parents=True, exist_ok=True)

        # Instanciation
        self.fetcher = MeteoFetcher(raw_dir=self.raw_dir)
        self.hourly_cleaner = HourlyCleaner()

    def _save_to_db(self, df: pd.DataFrame, table_name: str) -> None:
        if df.empty:
            print(f"⚠️ DataFrame pour {table_name} est vide. Ignoré.")
            return

        # Преобразуем все datetime в ISO строки
        for col in df.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns:
            df[col] = df[col].apply(lambda x: x.isoformat() if pd.notnull(x) else None)

        chunk_size = 500
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i+chunk_size].to_dict(orient="records")
            try:
                supabase.table(table_name).insert(chunk).execute()
                print(f"✨ {len(chunk)} lignes insérées dans {table_name}")
            except Exception as e:
                print(f"❌ Exception lors de l'insertion: {e}")


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

        # 4. SAUVEGARDE EN DB
        print("\n--- 4. Sauvegarde dans la base ---")
        self._save_to_db(hourly_hist_clean, table_name="meteo_history")
        self._save_to_db(hourly_fore_clean, table_name="meteo_forecast")
        
        print("\n✅ Pipeline terminé.")
        return {
            "hourly_history_rows": len(hourly_hist_clean),
            "hourly_forecast_rows": len(hourly_fore_clean),
        }
