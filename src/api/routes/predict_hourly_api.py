from fastapi import APIRouter, Query
from typing import List
from src.api.routes.prediction_final.predict_hourly import run_hourly_prediction
from src.api.utils.supabase_client import supabase

router = APIRouter()

@router.post("/predict/hourly")
async def predict_hourly(date: str = Query(..., description="YYYY-MM-DD")):
    """
    Generate hourly bike traffic prediction for all counters
    and store results in Supabase.
    """

    # 1. Calcul des prédictions
    df_pred = run_hourly_prediction(date)

    # 2. Conversion pour Supabase
    rows = df_pred.to_dict(orient="records")

    # 3. Insertion dans Supabas
    try:
        supabase.table("predictions_hourly").insert(rows).execute()
    except Exception as e:
        return {"status": "error", "message": str(e)}

    return {
        "status": "ok",
        "date": date,
        "records_inserted": len(rows)
    }
