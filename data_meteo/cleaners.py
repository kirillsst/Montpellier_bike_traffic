# meteo/cleaners.py
from dataclasses import dataclass
import pandas as pd

@dataclass
class HourlyCleaner:
    """Nettoyage des données horaires (Pluie + Temp + Vent)"""
    round_decimals: int = 2

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # 1. time → datetime
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
        df = df.dropna(subset=["time"])
        df = df.drop_duplicates(subset=["time"])
        df = df.sort_values("time").reset_index(drop=True)

        # 2. colonnes numériques
        num_cols = [
            "temperature_2m",
            "precipitation",
            "windspeed_10m",
        ]
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # 3. règles de cohérence
        if "precipitation" in df.columns:
            df.loc[df["precipitation"] < 0, "precipitation"] = 0
            
        if "windspeed_10m" in df.columns:
            df.loc[df["windspeed_10m"] < 0, "windspeed_10m"] = 0

        # 4. arrondi
        for col in num_cols:
            if col in df.columns:
                df[col] = df[col].round(self.round_decimals)

        return df