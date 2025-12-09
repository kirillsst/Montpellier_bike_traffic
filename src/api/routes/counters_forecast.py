# routes/forecast_pipeline.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from datetime import datetime, timedelta
from preparation_counters_forecast import extract, transform, load

router = APIRouter()

class ForecastRequest(BaseModel):
    date: str = None  # YYYY-MM-DD, if empty J+1

@router.post("/run-forecast-pipeline")
def run_forecast_pipeline(request: ForecastRequest):
    try:
        target_date = request.date or (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        
        # --- EXTRACT ---
        df_counters = extract.get_unique_counters()
        df_meteo = extract.get_meteo_forecast(target_date)
        calendar_info = extract.get_calendar_info(target_date)
        
        # --- TRANSFORM ---
        df_final = transform.build_forecast_dataset(df_counters, df_meteo, calendar_info, target_date)
        
        # --- LOAD ---
        load.upload_forecast_data(df_final)
        
        return {
            "status": "ok",
            "target_date": target_date,
            "rows_processed": len(df_final)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
