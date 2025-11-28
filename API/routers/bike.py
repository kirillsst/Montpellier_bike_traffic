# API/routers/bike.py

from datetime import date
from typing import Literal
from io import StringIO

import pandas as pd
import requests
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse

from .weather import fetch_hourly_history, DEFAULT_LAT, DEFAULT_LON
from .holidays import fetch_holidays_for_training

BASE_3M_API = "https://portail-api-data.montpellier3m.fr"

router = APIRouter(prefix="/bike", tags=["bike"])


def df_to_csv_response(df: pd.DataFrame, filename: str) -> StreamingResponse:
    buffer = StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def fetch_ecocounters(limit: int = 1000) -> pd.DataFrame:
    url = f"{BASE_3M_API}/ecocounter"
    params = {"limit": limit}
    r = requests.get(url, params=params)
    r.raise_for_status()
    raw = pd.DataFrame(r.json())

    def extract_value(x, key="value"):
        if isinstance(x, dict):
            return x.get(key)
        return None

    def extract_coordinates(x):
        if isinstance(x, dict) and "value" in x and "coordinates" in x["value"]:
            return x["value"]["coordinates"]
        return None

    df = pd.DataFrame()
    df["id"] = raw["id"]
    df["vehicle_type"] = raw["vehicleType"].apply(extract_value)
    df["intensity_sample"] = raw["intensity"].apply(extract_value)

    if "name" in raw.columns:
        df["display_name"] = raw["name"].apply(extract_value)
    else:
        df["display_name"] = None

    coords = raw["location"].apply(extract_coordinates)
    df["lon"] = coords.apply(lambda c: c[0] if c else None)
    df["lat"] = coords.apply(lambda c: c[1] if c else None)

    return df


def fetch_ecocounter_timeseries(
    ecocounter_id: str,
    from_date: str,
    to_date: str,
) -> pd.DataFrame:
    url = f"{BASE_3M_API}/ecocounter_timeseries/{ecocounter_id}/attrs/intensity"
    params = {"fromDate": from_date, "toDate": to_date}
    r = requests.get(url, params=params)
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"EcoCounter API error: {r.text}")

    ts = r.json()
    if "index" not in ts or "values" not in ts:
        raise HTTPException(status_code=500, detail="Unexpected timeseries format.")

    df = pd.DataFrame({
        "timestamp_utc": pd.to_datetime(ts["index"], utc=True),
        "intensity": ts["values"],
        "ecocounter_id": ts.get("entityId", ecocounter_id),
    })

    df["timestamp_local"] = df["timestamp_utc"].dt.tz_convert("Europe/Paris")
    df["date"] = df["timestamp_local"].dt.date.astype(str)
    df["hour"] = df["timestamp_local"].dt.floor("H")

    return df


@router.get("/counters")
def get_bike_counters(
    vehicle_type: str | None = Query(None),
    format: Literal["json", "csv"] = Query("json"),
):
    df = fetch_ecocounters()
    if vehicle_type is not None:
        df = df[df["vehicle_type"] == vehicle_type]
    if df.empty:
        raise HTTPException(status_code=404, detail="No counters found.")
    if format == "csv":
        return df_to_csv_response(df, "bike_counters.csv")
    return JSONResponse(df.to_dict(orient="records"))


@router.get("/timeseries")
def get_bike_timeseries(
    ecocounter_id: str = Query(...),
    from_date: str = Query("2023-01-01T00:00:00"),
    to_date: str = Query("2025-12-31T23:59:59"),
    format: Literal["json", "csv"] = Query("json"),
):
    df = fetch_ecocounter_timeseries(ecocounter_id, from_date, to_date)
    if df.empty:
        raise HTTPException(status_code=404, detail="No timeseries available.")
    if format == "csv":
        safe_id = ecocounter_id.replace(":", "_")
        return df_to_csv_response(df, f"bike_timeseries_{safe_id}.csv")
    return JSONResponse(df.to_dict(orient="records"))


@router.get("/timeseries-with-features")
def get_bike_timeseries_with_features(
    ecocounter_id: str = Query(...),
    from_date: str = Query("2023-01-01T00:00:00"),
    to_date: str = Query("2025-12-31T23:59:59"),
    zone: str = Query("metropole"),
    format: Literal["json", "csv"] = Query("json"),
):
    df_bike = fetch_ecocounter_timeseries(ecocounter_id, from_date, to_date)
    if df_bike.empty:
        raise HTTPException(status_code=404, detail="No bike data found.")

    start_day = df_bike["date"].min()
    end_day = df_bike["date"].max()

    df_weather = fetch_hourly_history(
        lat=DEFAULT_LAT,
        lon=DEFAULT_LON,
        start_date=start_day,
        end_date=end_day,
    )
    df_weather = df_weather.rename(columns={"time": "weather_time"})
    df_weather["weather_time"] = pd.to_datetime(df_weather["weather_time"])
    df_weather["weather_hour"] = df_weather["weather_time"].dt.floor("H")

    df_holidays = fetch_holidays_for_training(start_year=2023, zone=zone)
    df_holidays["date"] = df_holidays["date"].astype(str)

    merged = df_bike.merge(
        df_weather,
        left_on="hour",
        right_on="weather_hour",
        how="left",
    ).merge(
        df_holidays[["date", "name"]],
        on="date",
        how="left",
    )

    merged["is_holiday"] = merged["name"].notna()
    merged = merged.rename(columns={"name": "holiday_name"})
    merged = merged.drop(columns=["weather_hour"], errors="ignore")

    if format == "csv":
        safe_id = ecocounter_id.replace(":", "_")
        return df_to_csv_response(
            merged,
            f"bike_timeseries_features_{safe_id}_{start_day}_{end_day}.csv",
        )

    return JSONResponse(merged.to_dict(orient="records"))
