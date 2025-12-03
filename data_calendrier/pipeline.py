# data_calendrier/pipeline.py
from pathlib import Path
import pandas as pd

from api import HolidayFetcher
from clean import ContextGenerator

class CalendarPipeline:
    def __init__(self, output_dir: str = "data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.fetcher = HolidayFetcher(zone_scolaire="Zone C")
        self.generator = ContextGenerator()

    def run(self):
        print("--- DÉMARRAGE PIPELINE CALENDRIER ---")
        
        # 1. Extraction
        print("1. Appel APIs (Fériés & Vacances)...")
        df_feries = self.fetcher.fetch_feries(start_year=2023)
        df_vacances = self.fetcher.fetch_vacances()

        # 2. Transformation (Fusion)
        print("2. Construction du calendrier complet...")
        df_context = self.generator.process(df_feries, df_vacances)

        # 3. Sauvegarde
        filename = "calendrier_complet.csv"
        path = self.output_dir / filename
        df_context.to_csv(path, index=False)
        
        print(f"3. Sauvegarde terminée.")
        print(f"✅ Fichier généré : {path}")
        print(f"   - Lignes : {len(df_context)}")
        print(f"   - Colonnes : {list(df_context.columns)}")
        print(f"   - Période : {df_context['date'].min().date()} au {df_context['date'].max().date()}")