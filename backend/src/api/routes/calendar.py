from fastapi import APIRouter
from data_calendrier.main import run_pipeline

router = APIRouter()

@router.post("/run-calendar")
def run_calendar_route():
    try:
        result = run_pipeline()
        return {"status": "ok", "result": result}
    except Exception as e:
        return {"status": "error", "message": str(e)}
