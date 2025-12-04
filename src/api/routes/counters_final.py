# routes/final_dataset.py
from fastapi import APIRouter
from src.api.routes.final_dataset.pipeline import run_final_pipeline

router = APIRouter()

@router.post("/run-final-dataset")
def run_final_dataset_route():
    try:
        result = run_final_pipeline()
        return {"status": "ok", "result": result}
    except Exception as e:
        return {"status": "error", "message": str(e)}
