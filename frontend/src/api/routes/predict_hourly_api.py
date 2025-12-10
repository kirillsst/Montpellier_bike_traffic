from fastapi import APIRouter, Query
from datetime import datetime, timedelta
from src.api.utils.supabase_client import supabase
from src.api.routes.prediction_final.predict_hourly import run_prediction_pipeline

router = APIRouter()

@router.post("/predict/hourly")
async def predict_hourly(date: str | None = Query(
    None, description="YYYY-MM-DD (optional). If omitted, uses tomorrow (J+1)."
)):
    """
    Generate hourly bike traffic prediction for all counters.
    If date is not provided, automatically use tomorrow (J+1).
    """

    # If date not provided → set to J+1
    if date is None:
        date = (datetime.utcnow() + timedelta(days=1)).strftime("%Y-%m-%d")

    # 1. Run prediction pipeline
    try:
        predictions_list = run_prediction_pipeline(target_date=date)
    except Exception as e:
        return {"status": "error", "message": str(e)}

    if not predictions_list:
        return {"status": "error", "message": f"No predictions generated for {date}"}

    # 2. Clear table
    try:
        supabase.table("predictions_hourly").delete().neq("id", -1).execute()
    except Exception as e:
        return {"status": "error", "message": f"Failed to clear predictions_hourly: {e}"}

    # 3. Insert new predictions
    try:
        supabase.table("predictions_hourly").insert(predictions_list).execute()
    except Exception as e:
        return {"status": "error", "message": f"Failed to insert predictions: {e}"}

    return {
        "status": "ok",
        "date": date,
        "records_inserted": len(predictions_list),
        "message": "Hourly prediction completed and table refreshed"
    }
