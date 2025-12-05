from fastapi import APIRouter

router = APIRouter()

@router.get("/run-xgb-prediction")
def predict_example():
    return {"prediction": 123}
