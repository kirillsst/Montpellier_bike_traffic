# data_calendrier/api.py
import requests
import pandas as pd
from datetime import date, timedelta
import time

class HolidayFetcher:
    def __init__(self, zone_geo: str = "metropole", zone_scolaire: str = "Zone C"):
        self.zone_geo = zone_geo
        self.zone_scolaire = zone_scolaire
        self.base_url_feries = "https://calendrier.api.gouv.fr/jours-feries"
        
        # URL OFFICIELLE (Education Nationale)
        self.url_vacances = "https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/fr-en-calendrier-scolaire/exports/json"

        # --- CALCUL DE LA DATE LIMITE (Fin du mois précédent) ---
        today = date.today()
        # On prend le premier jour du mois actuel, et on retire 1 jour -> Dernier jour du mois d'avant
        self.end_date_limit = today.replace(day=1) - timedelta(days=1)
        
        print(f"   📅 Période Calendrier : 2023-01-01 -> {self.end_date_limit}")

    def fetch_feries(self, start_year: int = 2023) -> pd.DataFrame:
        """Récupère les jours fériés jusqu'à la fin du mois précédent."""
        
        # On ne récupère que les années concernées par la limite
        target_years = range(start_year, self.end_date_limit.year + 1)
        
        frames = []
        for year in target_years:
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
        
        # --- FILTRE DATE LIMITE ---
        # On convertit pour être sûr de bien comparer
        df_final['date'] = pd.to_datetime(df_final['date'])
        # On ne garde que ce qui est avant ou égal à la date limite
        df_final = df_final[df_final['date'].dt.date <= self.end_date_limit]
        
        return df_final

    def fetch_vacances(self) -> pd.DataFrame:
        """Récupère les vacances scolaires jusqu'à la fin du mois précédent."""
        print(f"   -> Vacances Scolaires ({self.zone_scolaire})...")
        
        # On récupère tout (historique)
        params = {
            "where": f'zones like "{self.zone_scolaire}"',
            "limit": -1,
            "timezone": "Europe/Paris"
        }
        
        try:
            r = requests.get(self.url_vacances, params=params, timeout=20)
            r.raise_for_status()
            data = r.json()
            
            df = pd.DataFrame(data)
            
            if not df.empty:
                # Colonnes utiles
                df = df[['description', 'start_date', 'end_date', 'zones']].copy()
                
                # --- FILTRE DATE LIMITE ---
                # Conversion robuste avec Timezone (UTC) puis retrait de la TZ pour comparer
                df['start_date'] = pd.to_datetime(df['start_date'], utc=True).dt.tz_localize(None)
                
                # On ne garde que les vacances qui ont COMMENCÉ avant ou à la date limite
                df = df[df['start_date'].dt.date <= self.end_date_limit]
                
                df = df.sort_values('start_date')
                
            return df
            
        except Exception as e:
            print(f"❌ Erreur Vacances : {e}")
            return pd.DataFrame()