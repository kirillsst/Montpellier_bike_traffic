from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routes.predict_hourly_api import router as predict_router
from .routes.archive import router as archive_router
from .routes.archive_clean import router as archive_clean_router
from .routes.meteo import router as meteo_router
from .routes.calendar import router as calendar_router
from .routes.counters_final import router as counters_final_router

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

app.include_router(predict_router, tags=["predict"])
app.include_router(archive_router, tags=["archive"])
app.include_router(archive_clean_router, tags=["archive_clean"])
app.include_router(meteo_router, tags=["meteo_router"])
app.include_router(calendar_router, tags=["calendar_router"])
app.include_router(counters_final_router, tags=["counters_final_router"])

@app.get("/health")
def root():
    return {"message": "Cyclable API is running"}