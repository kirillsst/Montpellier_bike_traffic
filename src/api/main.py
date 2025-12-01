from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routes.predict import router as predict_router
# from .routes.init import router as init_router
from .routes.archive import router as archive_router

app = FastAPI(
    title="Cyclable API",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_methods=["*"], 
    allow_headers=["*"],
)

app.include_router(predict_router, prefix="/predict")
# app.include_router(init_router, tags=["init"])
app.include_router(archive_router, tags=["archive"])

@app.get("/")
def root():
    return {"message": "Cyclable API is running"}