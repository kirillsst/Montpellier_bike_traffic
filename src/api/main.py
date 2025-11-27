from fastapi import FastAPI
from .routes.predict import router as predict_router
from .routes.init import router as init_router

app = FastAPI(
    title="Cyclable API",
    version="1.0.0",
)

app.include_router(predict_router, prefix="/predict")
app.include_router(init_router, tags=["init"])

@app.get("/")
def root():
    return {"message": "Cyclable API is running"}