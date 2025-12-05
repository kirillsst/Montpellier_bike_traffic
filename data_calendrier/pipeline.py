from pathlib import Path
import pandas as pd

from data_calendrier.api import HolidayFetcher
from data_calendrier.clean import ContextGenerator

from data_calendrier.supabase_client import supabase


class CalendarPipeline:
    def __init__(self, output_dir: str = "data", table_name="calendar"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.fetcher = HolidayFetcher(zone_scolaire="Zone C")
        self.generator = ContextGenerator()

        self.supabase = supabase
        self.table_name = table_name

    # --------------------------------------------------------
    # 3b. INSERTION DANS SUPABASE
    # --------------------------------------------------------
    def _save_to_db(self, df: pd.DataFrame):
        if df.empty:
            print("Le calendrier est vide — rien à sauvegarder.")
            return

        print("--- 3b. Nettoyage des dates ---")

        # Garantir format YYYY-MM-DD partout
        df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)

        # Convertir toutes les autres colonnes datetime
        for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
            df[col] = df[col].dt.date.astype(str)

        records = df.to_dict(orient="records")

        print(f"--- 3c. Vidage de la table avant insertion ({self.table_name}) ---")
        try:
            # Supprimer tout contenu de la table
            self.supabase.table(self.table_name).delete().neq("date", None).execute()
            print(" Table nettoyée")
        except Exception as e:
            print(f"  Erreur purge table : {e}")

        print(f"--- 3d. Insertion dans Supabase ({self.table_name}) ---")

        chunk_size = 2000  # performant pour des tables < 5k lignes

        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            try:
                self.supabase.table(self.table_name).insert(chunk).execute()
                print(f"   ✔ {len(chunk)} lignes insérées")
            except Exception as e:
                print(f" Erreur d'insertion : {e}")

    # --------------------------------------------------------
    # RUN
    # --------------------------------------------------------
    def run(self):
        print("--- DÉMARRAGE PIPELINE CALENDRIER ---")
        
        print("1. Appel APIs (Fériés & Vacances)...")
        df_feries = self.fetcher.fetch_feries(start_year=2023)
        df_vacances = self.fetcher.fetch_vacances()

        print("2. Construction du calendrier complet...")
        df_context = self.generator.process(df_feries, df_vacances)

        # Sauvegarde locale CSV
        filename = "calendrier_complet.csv"
        path = self.output_dir / filename
        df_context.to_csv(path, index=False)

        print(f"3a. CSV généré : {path}")
        print(f"   - Lignes : {len(df_context)}")
        print(f"   - Colonnes : {list(df_context.columns)}")
        print(f"   - Période : {df_context['date'].min()} → {df_context['date'].max()}")

        # Sauvegarde dans Supabase
        self._save_to_db(df_context)

        print("🎉 Pipeline terminé.\n")
