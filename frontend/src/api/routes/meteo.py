from fastapi import APIRouter
from data_meteo.main import run_pipeline

router = APIRouter()

@router.post("/run-meteo")
def run_meteo_route():
    try:
        result = run_pipeline()
        return {"status": "ok", "result": result}
    except Exception as e:
        return {"status": "error", "message": str(e)}
