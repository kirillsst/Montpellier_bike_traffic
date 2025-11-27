from fastapi import APIRouter
from src.api.utils.fetch_ecocounter import (
    fetch_all_counters,
    fetch_all_timeseries
)

router = APIRouter()


@router.post("/init/load")
def init_load():
    """
    Initialisation de l'archive : télécharger TOUS les capteurs + archives.
    """
    # 1 — liste des capteurs
    df_counters = fetch_all_counters()

    # 2 — archive intensité
    df_ts = fetch_all_timeseries(df_counters)

    df_counters.to_csv("data/raw/counters.csv", index=False)
    df_ts.to_csv("data/raw/velocounter.csv", index=False)

    return {
        "status": "OK",
        "counters": len(df_counters),
        "rows": len(df_ts)
    }
