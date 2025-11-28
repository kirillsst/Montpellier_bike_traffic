# API/routers/weather.py

from datetime import date, timedelta
from typing import Literal
from io import StringIO

import pandas as pd
import requests
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse

TIMEZONE = "Europe/Paris"
DEFAULT_LAT = 43.6
DEFAULT_LON = 3.88
DEFAULT_START_DATE = "2023-01-01"
DEFAULT_END_DATE = date.today().isoformat()

router = APIRouter(prefix="/weather", tags=["weather"])


def df_to_csv_response(df: pd.DataFrame, filename: str) -> StreamingResponse:
    buffer = StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def fetch_hourly_history(lat: float, lon: float, start_date: str, end_date: str) -> pd.DataFrame:
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,relativehumidity_2m,precipitation,windspeed_10m",
        "timezone": TIMEZONE,
    }
    r = requests.get(url, params=params)
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Open-Meteo archive error: {r.text}")
    d = r.json()["hourly"]
    df = pd.DataFrame(d)
    df["time"] = pd.to_datetime(df["time"])
    return df


def fetch_hourly_forecast(lat: float, lon: float) -> pd.DataFrame:
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,relativehumidity_2m,precipitation,windspeed_10m",
        "forecast_days": 2,
        "timezone": TIMEZONE,
    }
    r = requests.get(url, params=params)
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Open-Meteo forecast error: {r.text}")
    d = r.json()["hourly"]
    df = pd.DataFrame(d)
    df["time"] = pd.to_datetime(df["time"])
    return df


def fetch_daily_history(lat: float, lon: float, start_date: str, end_date: str) -> pd.DataFrame:
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": (
            "temperature_2m_mean,temperature_2m_max,temperature_2m_min,"
            "precipitation_sum,windspeed_10m_max,windspeed_10m_mean"
        ),
        "timezone": TIMEZONE,
    }
    r = requests.get(url, params=params)
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Open-Meteo archive error: {r.text}")
    d = r.json()["daily"]
    df = pd.DataFrame(d)
    df["time"] = pd.to_datetime(df["time"])
    return df


def fetch_daily_forecast(lat: float, lon: float) -> pd.DataFrame:
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": (
            "temperature_2m_mean,temperature_2m_max,temperature_2m_min,"
            "precipitation_sum,windspeed_10m_max,windspeed_10m_mean"
        ),
        "forecast_days": 2,
        "timezone": TIMEZONE,
    }
    r = requests.get(url, params=params)
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Open-Meteo forecast error: {r.text}")
    d = r.json()["daily"]
    df = pd.DataFrame(d)
    df["time"] = pd.to_datetime(df["time"])
    return df


@router.get("/hourly/history")
def get_hourly_history(
    lat: float = Query(DEFAULT_LAT),
    lon: float = Query(DEFAULT_LON),
    start_date: str = Query(DEFAULT_START_DATE),
    end_date: str = Query(DEFAULT_END_DATE),
    format: Literal["json", "csv"] = Query("json"),
):
    df = fetch_hourly_history(lat, lon, start_date, end_date)
    if format == "csv":
        return df_to_csv_response(df, f"hourly_history_{start_date}_{end_date}.csv")
    return JSONResponse(df.to_dict(orient="records"))


@router.get("/hourly/forecast-tomorrow")
def get_hourly_forecast_tomorrow(
    lat: float = Query(DEFAULT_LAT),
    lon: float = Query(DEFAULT_LON),
    format: Literal["json", "csv"] = Query("json"),
):
    df = fetch_hourly_forecast(lat, lon)
    tomorrow = date.today() + timedelta(days=1)
    df_tomorrow = df[df["time"].dt.date == tomorrow]
    if df_tomorrow.empty:
        raise HTTPException(status_code=404, detail="No hourly forecast for tomorrow.")
    if format == "csv":
        return df_to_csv_response(df_tomorrow, f"hourly_forecast_{tomorrow.isoformat()}.csv")
    return JSONResponse(df_tomorrow.to_dict(orient="records"))


@router.get("/daily/history")
def get_daily_history(
    lat: float = Query(DEFAULT_LAT),
    lon: float = Query(DEFAULT_LON),
    start_date: str = Query(DEFAULT_START_DATE),
    end_date: str = Query(DEFAULT_END_DATE),
    format: Literal["json", "csv"] = Query("json"),
):
    df = fetch_daily_history(lat, lon, start_date, end_date)
    if format == "csv":
        return df_to_csv_response(df, f"daily_history_{start_date}_{end_date}.csv")
    return JSONResponse(df.to_dict(orient="records"))


@router.get("/daily/forecast-tomorrow")
def get_daily_forecast_tomorrow(
    lat: float = Query(DEFAULT_LAT),
    lon: float = Query(DEFAULT_LON),
    format: Literal["json", "csv"] = Query("json"),
):
    df = fetch_daily_forecast(lat, lon)
    tomorrow = date.today() + timedelta(days=1)
    df_tomorrow = df[df["time"].dt.date == tomorrow]
    if df_tomorrow.empty:
        raise HTTPException(status_code=404, detail="No daily forecast for tomorrow.")
    if format == "csv":
        return df_to_csv_response(df_tomorrow, f"daily_forecast_{tomorrow.isoformat()}.csv")
    return JSONResponse(df_tomorrow.to_dict(orient="records"))
