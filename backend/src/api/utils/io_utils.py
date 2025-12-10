import pandas as pd
import os

def load_and_pivot_local_csv(filepath):
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Le fichier {filepath} est introuvable.")

    df_raw = pd.read_csv(filepath, sep=';', encoding='latin1')
    df_raw = df_raw.iloc[:, :5]
    df_raw.columns = ["nom_csv", "serial_a", "serial_b", "latitude", "longitude"]

    for col in ["latitude", "longitude"]:
        df_raw[col] = df_raw[col].astype(str).str.replace(',', '.', regex=False)
        df_raw[col] = pd.to_numeric(df_raw[col], errors='coerce')

    df_part_a = df_raw[["nom_csv", "serial_a", "latitude", "longitude"]].rename(columns={"serial_a": "serial_number"})
    df_part_b = df_raw[["nom_csv", "serial_b", "latitude", "longitude"]].rename(columns={"serial_b": "serial_number"})
    
    df_combined = pd.concat([df_part_a, df_part_b], ignore_index=True)
    df_combined = df_combined.dropna(subset=["serial_number"])
    df_combined["serial_number"] = df_combined["serial_number"].astype(str).str.strip()
    df_combined = df_combined[df_combined["serial_number"] != "nan"]
    df_combined = df_combined.drop_duplicates(subset=["serial_number"])
    
    return df_combined
