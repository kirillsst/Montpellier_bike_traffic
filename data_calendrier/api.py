# data_calendrier/api.py
import requests
import pandas as pd
from datetime import date
import time

class HolidayFetcher:
    def __init__(self, zone_geo: str = "metropole", zone_scolaire: str = "Zone C"):
        self.zone_geo = zone_geo
        self.zone_scolaire = zone_scolaire
        self.base_url_feries = "https://calendrier.api.gouv.fr/jours-feries"
        self.url_vacances = "https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/fr-en-calendrier-scolaire/exports/json"

        # Fin fixe pour tout 2025
        self.end_date_limit = date(2025, 12, 31)
        print(f"   📅 Période Calendrier : 2023-01-01 -> {self.end_date_limit}")

    def fetch_feries(self, start_year: int = 2023) -> pd.DataFrame:
        frames = []
        for year in range(start_year, self.end_date_limit.year + 1):
            url = f"{self.base_url_feries}/{self.zone_geo}/{year}.json"
            try:
                print(f"   -> Fériés {year}...")
                r = requests.get(url, timeout=10)
                if r.status_code == 200:
                    df = pd.DataFrame(list(r.json().items()), columns=['date', 'nom_ferie'])
                    frames.append(df)
            except Exception:
                pass
            time.sleep(0.1)

        if not frames:
            return pd.DataFrame()

        df_final = pd.concat(frames, ignore_index=True)
        df_final['date'] = pd.to_datetime(df_final['date'])
        df_final = df_final[df_final['date'].dt.date <= self.end_date_limit]
        return df_final

    def fetch_vacances(self) -> pd.DataFrame:
        print(f"   -> Vacances Scolaires ({self.zone_scolaire})...")
        params = {
            "where": f'zones like "{self.zone_scolaire}"',
            "limit": -1,
            "timezone": "Europe/Paris"
        }
        try:
            r = requests.get(self.url_vacances, params=params, timeout=20)
            r.raise_for_status()
            df = pd.DataFrame(r.json())
            if not df.empty:
                df = df[['description', 'start_date', 'end_date', 'zones']].copy()
                df['start_date'] = pd.to_datetime(df['start_date'], utc=True).dt.tz_localize(None)
                df['end_date'] = pd.to_datetime(df['end_date'], utc=True).dt.tz_localize(None)
                df = df[df['start_date'].dt.date <= self.end_date_limit]
                df = df.sort_values('start_date')
            return df
        except Exception as e:
            print(f"❌ Erreur Vacances : {e}")
            return pd.DataFrame()
