import requests
import pandas as pd
from tqdm import tqdm

BASE = "https://portail-api-data.montpellier3m.fr"


def fetch_all_counters():
    """Obtenir la liste de tous les capteurs EcoCounter."""
    url = f"{BASE}/ecocounter"
    data = requests.get(url).json()
    return pd.DataFrame(data)


def fetch_timeseries(counter_id: str):
    """
    Charge la série temporelle intensity pour un capteur.
    Renvoie pd.DataFrame.
    """
    url = f"{BASE}/ecocounter_timeseries/{counter_id}/attrs/intensity"
    ts = requests.get(url).json()

    df = pd.DataFrame({
        "timestamp": pd.to_datetime(ts["index"]),
        "intensity": ts["values"],
        "ecocounter_id": ts["entityId"]
    })

    return df


def fetch_all_timeseries(df_counters: pd.DataFrame):
    """Charge l'archive intensity pour tous les capteurs."""
    all_dfs = []

    for cid in tqdm(df_counters["id"], desc="Downloading timeseries"):
        try:
            df_ts = fetch_timeseries(cid)
            all_dfs.append(df_ts)
        except Exception as e:
            print(f"Erreur de chargement {cid}: {e}")

    if len(all_dfs) == 0:
        raise ValueError("Impossible de charger une seule série chronologique")

    return pd.concat(all_dfs, ignore_index=True)
