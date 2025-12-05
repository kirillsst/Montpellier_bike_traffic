# data_calendrier/clean.py
import pandas as pd
from datetime import date, timedelta

class ContextGenerator:
    def process(self, df_feries: pd.DataFrame, df_vacances: pd.DataFrame) -> pd.DataFrame:
        print("   -> Génération de la grille temporelle continue...")
        
        # 1. Calcul dynamique de la fin (Fin du mois précédent)
        today = date.today()
        # Premier jour du mois actuel - 1 jour = Dernier jour du mois précédent
        end_date_limit = today.replace(day=1) - timedelta(days=1)
        
        start_date = "2023-01-01"
        print(f"      Période générée : {start_date} au {end_date_limit}")
        
        # On crée la grille jusqu'à la date limite
        all_days = pd.date_range(start=start_date, end=end_date_limit, freq='D')
        
        df = pd.DataFrame({"date": all_days})
        
        # 2. Features de base
        df["jour_semaine"] = df["date"].dt.dayofweek
        df["is_weekend"] = df["jour_semaine"] >= 5
        df["nom_jour"] = df["date"].dt.day_name()

        # 3. Intégration Fériés
        if not df_feries.empty:
            df_feries["date"] = pd.to_datetime(df_feries["date"])
            df = df.merge(df_feries, on="date", how="left")
            df["is_ferie"] = df["nom_ferie"].notna()
        else:
            df["is_ferie"] = False

        # 4. Intégration Vacances Scolaires
        df["is_vacances"] = False
        
        if not df_vacances.empty:
            # Correction Timezone (utc=True)
            df_vacances["start_date"] = pd.to_datetime(df_vacances["start_date"], utc=True).dt.tz_localize(None)
            df_vacances["end_date"] = pd.to_datetime(df_vacances["end_date"], utc=True).dt.tz_localize(None)
            
            for _, row in df_vacances.iterrows():
                # On ne garde que les segments qui chevauchent notre période
                mask = (df["date"] >= row["start_date"]) & (df["date"] <= row["end_date"])
                df.loc[mask, "is_vacances"] = True

        # 5. Calcul Jour Ouvré
        df["is_jour_ouvre"] = (~df["is_weekend"]) & (~df["is_ferie"])

        # 6. Nettoyage final (Typage Int)
        bool_cols = ["is_weekend", "is_ferie", "is_vacances", "is_jour_ouvre"]
        df[bool_cols] = df[bool_cols].astype(int)

        # 7. SÉLECTION DES COLONNES FINALES
        final_cols = [
            "date", 
            "jour_semaine", 
            "is_weekend", 
            "nom_jour", 
            "is_ferie", 
            "is_vacances", 
            "is_jour_ouvre"
        ]
        
        return df[final_cols]