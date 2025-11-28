# API/app.py

from fastapi import FastAPI

# Import routers from the routers directory
from .routers.weather import router as weather_router
from .routers.holidays import router as holidays_router
from .routers.bike import router as bike_router

app = FastAPI(
    title="Montpellier Bike & Weather API",
    version="1.0.0",
    description="Weather, holidays, and bike traffic data API for Montpellier.",
)

# Register routers
app.include_router(weather_router)
app.include_router(holidays_router)
app.include_router(bike_router)
