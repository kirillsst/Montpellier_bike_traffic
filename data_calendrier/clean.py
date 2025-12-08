# data_calendrier/clean.py
import pandas as pd
from datetime import date

class ContextGenerator:
    def process(self, df_feries: pd.DataFrame, df_vacances: pd.DataFrame) -> pd.DataFrame:
        start_date = "2023-01-01"
        end_date_limit = date(2025, 12, 31)
        print(f"   -> Génération de la grille temporelle continue : {start_date} -> {end_date_limit}")

        all_days = pd.date_range(start=start_date, end=end_date_limit, freq='D')
        df = pd.DataFrame({"date": all_days})

        df["jour_semaine"] = df["date"].dt.dayofweek
        df["is_weekend"] = df["jour_semaine"] >= 5
        df["nom_jour"] = df["date"].dt.day_name()

        if not df_feries.empty:
            df_feries["date"] = pd.to_datetime(df_feries["date"])
            df = df.merge(df_feries, on="date", how="left")
            df["is_ferie"] = df["nom_ferie"].notna()
        else:
            df["is_ferie"] = False

        df["is_vacances"] = False
        if not df_vacances.empty:
            for _, row in df_vacances.iterrows():
                mask = (df["date"] >= row["start_date"]) & (df["date"] <= row["end_date"])
                df.loc[mask, "is_vacances"] = True

        df["is_jour_ouvre"] = (~df["is_weekend"]) & (~df["is_ferie"])
        bool_cols = ["is_weekend", "is_ferie", "is_vacances", "is_jour_ouvre"]
        df[bool_cols] = df[bool_cols].astype(int)

        final_cols = [
            "date", "jour_semaine", "is_weekend", "nom_jour", 
            "is_ferie", "is_vacances", "is_jour_ouvre"
        ]
        return df[final_cols]
