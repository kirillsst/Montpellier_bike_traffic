# meteo/cleaners.py
from dataclasses import dataclass
import pandas as pd

@dataclass
class HourlyCleaner:
    """Nettoyage des données horaires (Pluie + Temp + Vent + Classification)"""
    round_decimals: int = 2

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # --- 1. Gestion du temps (Identique) ---
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
        df = df.dropna(subset=["time"])
        df = df.drop_duplicates(subset=["time"])
        df = df.sort_values("time").reset_index(drop=True)

        # --- 2. Conversion numérique (Identique) ---
        num_cols = [
            "temperature_2m",
            "precipitation",
            "windspeed_10m",
        ]
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # --- 3. Règles de cohérence physique (Identique) ---
        if "precipitation" in df.columns:
            df.loc[df["precipitation"] < 0, "precipitation"] = 0
            
        if "windspeed_10m" in df.columns:
            df.loc[df["windspeed_10m"] < 0, "windspeed_10m"] = 0

        # --- 4. Arrondis (Identique) ---
        for col in num_cols:
            if col in df.columns:
                df[col] = df[col].round(self.round_decimals)

        # --- 5. CLASSIFICATION OPTIMISÉE VÉLO (0, 1, 2, 3) ---
        if "precipitation" in df.columns:
            # Par défaut 0 (Sec)
            df["precipitation_class"] = 0
            
            # Classe 1 : Bruine / Traces (Impact psychologique faible)
            mask_faible = (df["precipitation"] > 0) & (df["precipitation"] < 0.5)
            df.loc[mask_faible, "precipitation_class"] = 1
            
            # Classe 2 : Pluie avérée (Impact fort, c'est le seuil critique)
            mask_moderee = (df["precipitation"] >= 0.5) & (df["precipitation"] < 4)
            df.loc[mask_moderee, "precipitation_class"] = 2
            
            # Classe 3 : Forte pluie (Impact total)
            mask_forte = (df["precipitation"] >= 4)
            df.loc[mask_forte, "precipitation_class"] = 3

        # --- 6. NOUVEAUTÉ : BINAIRE PLUIE (OUI/NON) ---
        # 1 = Il pleut (même un tout petit peu, > 0mm)
        # 0 = Il ne pleut pas (0mm)
        if "precipitation" in df.columns:
            df["is_raining"] = (df["precipitation"] > 0).astype(int)

        return df