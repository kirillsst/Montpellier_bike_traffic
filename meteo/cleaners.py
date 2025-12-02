# meteo/cleaners.py
from dataclasses import dataclass
import pandas as pd


@dataclass
class DailyCleaner:
    """
    Nettoyage des données journalières (history + forecast)
    """
    round_decimals: int = 2

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # 1. time → date
        df["time"] = pd.to_datetime(df["time"], errors="coerce").dt.date
        df = df.dropna(subset=["time"])
        df = df.drop_duplicates(subset=["time"])
        df = df.sort_values("time").reset_index(drop=True)

        # 2. colonnes numériques
        num_cols = [
            "temperature_2m_mean",
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "windspeed_10m_max",
            "windspeed_10m_mean",
        ]
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # 3. règles simples
        if "precipitation_sum" in df.columns:
            df.loc[df["precipitation_sum"] < 0, "precipitation_sum"] = 0

        # 4. arrondi
        for col in num_cols:
            if col in df.columns:
                df[col] = df[col].round(self.round_decimals)

        return df


@dataclass
class HourlyCleaner:
    """
    Nettoyage des données horaires (history + forecast)
    """
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
            "relativehumidity_2m",
            "precipitation",
            "windspeed_10m",
        ]
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # 3. règles de cohérence
        if "relativehumidity_2m" in df.columns:
            mask_bad = ~df["relativehumidity_2m"].between(0, 100)
            df.loc[mask_bad, "relativehumidity_2m"] = pd.NA

        if "precipitation" in df.columns:
            df.loc[df["precipitation"] < 0, "precipitation"] = 0

        if "windspeed_10m" in df.columns:
            df.loc[df["windspeed_10m"] < 0, "windspeed_10m"] = 0

        # 4. arrondi
        for col in ["temperature_2m", "precipitation", "windspeed_10m"]:
            if col in df.columns:
                df[col] = df[col].round(self.round_decimals)

        return df
