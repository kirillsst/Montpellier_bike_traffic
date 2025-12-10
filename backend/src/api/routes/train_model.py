# src/api/routes/pipeline_xgboost.py
from fastapi import APIRouter
from train_model_xgboost import pipeline_train
import pandas as pd

router = APIRouter()

@router.post("/pipeline/xgboost/run")
async def run_xgboost_pipeline_route():
    """
    Trigger the full XGBoost pipeline:
    - Load dataset
    - Train models for each counter
    - Evaluate
    - Save models and metrics
    """
    try:
        pipeline_train.run_xgboost_pipeline()

        return {
            "status": "ok",
            "message": "XGBoost pipeline executed successfully. Check logs for details."
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }
