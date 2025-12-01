import requests
import pandas as pd
from tqdm import tqdm
from pathlib import Path
import datetime

# ==============================
# CONFIG
# ==============================
BASE = "https://portail-api-data.montpellier3m.fr"
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# Date de fin (aujourd'hui par défaut)
DATE_FIN = datetime.datetime.utcnow().replace(minute=0, second=0, microsecond=0)
DATE_FIN_STR = DATE_FIN.strftime("%Y-%m-%dT%H:%M:%S")

PERIODES = [
    ("2023-01-01T00:00:00", "2023-12-31T23:59:59"),
    ("2024-01-01T00:00:00", "2024-12-31T23:59:59"),
    ("2025-01-01T00:00:00", DATE_FIN_STR),
]


# ==============================
# HELPERS
# ==============================

def fetch_all_counters():
    """
    Récupère tous les compteurs EcoCounter via pagination.
    Renvoie un DataFrame avec les colonnes brutes.
    """
    print("📡 Téléchargement de la liste des compteurs...")
    url = f"{BASE}/ecocounter"
    limit = 1000
    offset = 0
    all_results = []

    while True:
        full_url = f"{url}?limit={limit}&offset={offset}"
        resp = requests.get(full_url, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if not isinstance(data, list) or len(data) == 0:
            break

        all_results.extend(data)
        offset += limit

    df = pd.DataFrame(all_results)

    # Extraire le numéro de série depuis l'ID
    df["serial_number"] = df["id"].apply(lambda x: x.split(":")[-1] if isinstance(x, str) else None)
    return df


def load_geo_reference(filepath):
    """
    Charge le CSV local et renvoie un DataFrame avec:
    serial_number | name | lat | lon
    """

    df_raw = pd.read_csv(filepath, sep=";", encoding="latin1")

    df_raw = df_raw.iloc[:, [0, 1, 2, 3, 4]]
    df_raw.columns = ["nom_csv", "serial_a", "serial_b", "latitude", "longitude"]

    # Nettoyage lat/lon
    df_raw["latitude"] = df_raw["latitude"].astype(str).str.replace(",", ".", regex=False)
    df_raw["longitude"] = df_raw["longitude"].astype(str).str.replace(",", ".", regex=False)
    df_raw["latitude"] = pd.to_numeric(df_raw["latitude"], errors="coerce")
    df_raw["longitude"] = pd.to_numeric(df_raw["longitude"], errors="coerce")

    # Pivot serial_a / serial_b
    df_a = df_raw[["nom_csv", "serial_a", "latitude", "longitude"]].rename(columns={"serial_a": "serial_number"})
    df_b = df_raw[["nom_csv", "serial_b", "latitude", "longitude"]].rename(columns={"serial_b": "serial_number"})

    df_geo = pd.concat([df_a, df_b], ignore_index=True)
    df_geo = df_geo.dropna(subset=["serial_number"])
    df_geo["serial_number"] = df_geo["serial_number"].astype(str).str.strip()
    df_geo = df_geo.drop_duplicates(subset=["serial_number"])

    return df_geo


def fetch_timeseries(counter_id, start_date, end_date):
    """
    Télécharge une période pour un compteur.
    """
    params = {
        "fromDate": start_date,
        "toDate": end_date
    }

    url = f"{BASE}/ecocounter_timeseries/{counter_id}/attrs/intensity"
    resp = requests.get(url, params=params, timeout=60)

    if resp.status_code != 200:
        return None

    ts = resp.json()
    if "index" not in ts or "values" not in ts:
        return None

    return pd.DataFrame({
        "timestamp": pd.to_datetime(ts["index"], utc=True),
        "intensity": ts["values"]
    })


# ==============================
# MAIN ETL
# ==============================

def run_etl(geo_csv_path):
    print("🚀 DÉMARRAGE ETL BIKE DATA")

    df_geo = load_geo_reference(geo_csv_path)
    df_api = fetch_all_counters()

    # Fusion par numéro de série
    df_merged = df_api.merge(df_geo, on="serial_number", how="inner")

    print(f"🟢 Compteurs avec correspondance géo trouvés : {len(df_merged)}")

    all_dfs = []

    for _, row in tqdm(df_merged.iterrows(), total=len(df_merged), desc="Téléchargement séries"):
        cid = row["id"]

        for start_date, end_date in PERIODES:
            df_ts = fetch_timeseries(cid, start_date, end_date)
            if df_ts is not None:
                df_ts["name"] = row["nom_csv"]
                df_ts["latitude"] = row["latitude"]
                df_ts["longitude"] = row["longitude"]
                all_dfs.append(df_ts)

    if len(all_dfs) == 0:
        print("❌ Aucune série téléchargée.")
        return

    df_concat = pd.concat(all_dfs, ignore_index=True)

    # Group + clean
    df_final = (
        df_concat.groupby(["name", "latitude", "longitude", "timestamp"], as_index=False)
        .agg({"intensity": "sum"})
        .sort_values(["name", "timestamp"])
    )

    # Sauvegarde
    df_final.to_parquet(DATA_DIR / "bike_full.parquet", index=False)
    df_final.to_csv(DATA_DIR / "bike_full.csv", index=False)

    print(f"✅ ETL TERMINÉ — {len(df_final)} lignes sauvées dans data/bike_full.*")


if __name__ == "__main__":
    GEO_CSV_PATH = "/path/to/Géoloc_Comtpeurs.csv"   # TODO: change this
    run_etl(GEO_CSV_PATH)
