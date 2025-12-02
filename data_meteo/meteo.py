# meteo/meteo.py
import requests
import pandas as pd
from datetime import date, timedelta
from pathlib import Path

# Configuration
TIMEZONE = "Europe/Paris"
LAT = 43.6107  # Montpellier
LON = 3.8767
START_DATE = "2023-01-01"

class MeteoFetcher:
    def __init__(self, raw_dir: str = "data/raw"):
        self.raw_dir = Path(raw_dir)
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def _fetch_api(self, url: str, params: dict) -> pd.DataFrame:
        """Appel générique à l'API."""
        try:
            r = requests.get(url, params=params)
            r.raise_for_status()
            
            # On ne s'attend plus qu'à du 'hourly'
            if "hourly" in r.json():
                df = pd.DataFrame(r.json()["hourly"])
                return df
            return pd.DataFrame()
        except Exception as e:
            print(f"❌ Erreur API : {e}")
            return pd.DataFrame()

    def download_all(self) -> dict:
        print("--- 1. Extraction des données Météo (HORAIRE uniquement) ---")
        
        today = date.today()
        yesterday = today - timedelta(days=1)
        
        url_archive = "https://archive-api.open-meteo.com/v1/archive"
        url_forecast = "https://api.open-meteo.com/v1/forecast"
        
        # 1. Historique (Hourly)
        # On récupère Température, Pluie, Vent
        df_hourly_hist = self._fetch_api(url_archive, {
            "latitude": LAT, "longitude": LON,
            "start_date": START_DATE, "end_date": yesterday.isoformat(),
            "hourly": "temperature_2m,precipitation,windspeed_10m",
            "timezone": TIMEZONE
        })

        # 2. Prévision (Hourly) - 4 jours
        df_hourly_fore = self._fetch_api(url_forecast, {
            "latitude": LAT, "longitude": LON,
            "hourly": "temperature_2m,precipitation,windspeed_10m",
            "forecast_days": 4, "timezone": TIMEZONE
        })

        # Sauvegarde Brute
        self._save_raw(df_hourly_hist, "raw_hourly_history.csv")
        self._save_raw(df_hourly_fore, "raw_hourly_forecast.csv")
        
        return {}

    def _save_raw(self, df: pd.DataFrame, filename: str) -> Path:
        path = self.raw_dir / filename
        df.to_csv(path, index=False)
        return path