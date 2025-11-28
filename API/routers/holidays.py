# API/routers/holidays.py

from datetime import date
from typing import Literal
from io import StringIO

import pandas as pd
import requests
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse

router = APIRouter(prefix="/holidays", tags=["holidays"])


def df_to_csv_response(df: pd.DataFrame, filename: str) -> StreamingResponse:
    buffer = StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def fetch_french_holidays(year: int, zone: str = "metropole") -> pd.DataFrame:
    url = f"https://calendrier.api.gouv.fr/jours-feries/{year}/{zone}.json"
    r = requests.get(url)
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Holidays API error: {r.text}")
    data = r.json()
    df = pd.DataFrame(
        [{"date": d, "name": name, "zone": zone, "year": year} for d, name in data.items()]
    ).sort_values("date")
    return df.reset_index(drop=True)


def fetch_current_year_holidays(zone: str = "metropole") -> pd.DataFrame:
    current_year = date.today().year
    return fetch_french_holidays(current_year, zone)


def fetch_holidays_for_training(start_year: int = 2023, zone: str = "metropole") -> pd.DataFrame:
    current_year = date.today().year
    frames = [fetch_french_holidays(y, zone) for y in range(start_year, current_year + 1)]
    df_all = (
        pd.concat(frames, ignore_index=True)
        .sort_values("date")
        .reset_index(drop=True)
    )
    return df_all


@router.get("/current-year")
def get_holidays_current_year(
    zone: str = Query("metropole"),
    format: Literal["json", "csv"] = Query("json"),
):
    df = fetch_current_year_holidays(zone)
    if format == "csv":
        year = date.today().year
        return df_to_csv_response(df, f"holidays_{zone}_{year}.csv")
    return JSONResponse(df.to_dict(orient="records"))


@router.get("/training")
def get_holidays_for_training_endpoint(
    start_year: int = Query(2023),
    zone: str = Query("metropole"),
    format: Literal["json", "csv"] = Query("json"),
):
    df = fetch_holidays_for_training(start_year=start_year, zone=zone)
    if format == "csv":
        current_year = date.today().year
        return df_to_csv_response(df, f"holidays_{zone}_{start_year}_{current_year}.csv")
    return JSONResponse(df.to_dict(orient="records"))
