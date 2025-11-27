from fastapi import APIRouter

router = APIRouter()

@router.get("/")
def predict_example():
    return {"prediction": 123}
