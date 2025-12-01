# API/routers/bike.py

import requests
import pandas as pd
from io import StringIO
from typing import Literal

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse

BASE_3M_API = "https://portail-api-data.montpellier3m.fr"

router = APIRouter(prefix="/bike", tags=["bike"])


def df_to_csv_response(df: pd.DataFrame, filename: str) -> StreamingResponse:
    """
    Stream a DataFrame as a CSV file in the HTTP response.
    """
    buffer = StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def df_to_json_records(df: pd.DataFrame) -> list[dict]:
    """
    Convert a DataFrame to a JSON-serializable list of dicts.
    (Convert datetime columns to strings if needed.)
    """
    df_out = df.copy()
    for col in df_out.select_dtypes(
        include=["datetime64[ns]", "datetime64[ns, UTC]"]
    ).columns:
        df_out[col] = df_out[col].astype(str)
    return df_out.to_dict(orient="records")


def fetch_all_counters_flat() -> pd.DataFrame:
    """
    Récupère tous les EcoCounters depuis l'API Montpellier et
    renvoie un DataFrame aplati avec les colonnes suivantes :

    - id             : identifiant complet (urn:ngsi-ld:EcoCounter:...)
    - serial_number  : dernière partie de l'ID (X2H..., etc.)
    - vehicle_type   : 'bicycle', 'pedestrian', etc.
    - display_name   : nom lisible du compteur si disponible
    - lat, lon       : coordonnées WGS84 (latitude, longitude)
    """
    url = f"{BASE_3M_API}/ecocounter"
    limit = 1000
    offset = 0
    all_results: list[dict] = []

    while True:
        full_url = f"{url}?limit={limit}&offset={offset}"
        resp = requests.get(full_url, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        # L'API renvoie une liste; si vide, fin de pagination
        if not isinstance(data, list) or len(data) == 0:
            break

        all_results.extend(data)
        offset += limit

    raw = pd.DataFrame(all_results)

    def extract_value(x):
        if isinstance(x, dict):
            return x.get("value")
        return None

    def extract_coords(x):
        """
        location.value.coordinates est de la forme [lon, lat]
        """
        if (
            isinstance(x, dict)
            and "value" in x
            and isinstance(x["value"], dict)
            and "coordinates" in x["value"]
        ):
            coords = x["value"]["coordinates"]
            if isinstance(coords, (list, tuple)) and len(coords) >= 2:
                lon, lat = coords[0], coords[1]
                return lat, lon
        return None, None

    df = pd.DataFrame()
    df["id"] = raw["id"]

    # serial_number = dernière partie de l'ID après les ':'
    df["serial_number"] = df["id"].apply(
        lambda x: x.split(":")[-1] if isinstance(x, str) else None
    )

    # type de véhicule (bicycle / pedestrian / etc.)
    if "vehicleType" in raw.columns:
        df["vehicle_type"] = raw["vehicleType"].apply(extract_value)
    else:
        df["vehicle_type"] = None

    # displayName s'il existe, sinon name, sinon None
    if "displayName" in raw.columns:
        df["display_name"] = raw["displayName"].apply(extract_value)
    elif "name" in raw.columns:
        df["display_name"] = raw["name"].apply(extract_value)
    else:
        df["display_name"] = None

    # coordonnées
    lats, lons = zip(*raw["location"].apply(extract_coords))
    df["lat"] = lats
    df["lon"] = lons

    return df


@router.get("/geoloc")
def get_bike_geolocations(
    vehicle_type: str | None = Query(
        None,
        description="Filtrer par type de véhicule, ex: 'bicycle'. Laisser vide = tous.",
    ),
    format: Literal["json", "csv"] = Query(
        "json",
        description="Format de sortie. Utiliser 'csv' pour télécharger un fichier.",
    ),
):
    """
    Retourne la liste des compteurs EcoCounter avec géolocalisation et infos de base.

    Colonnes retournées :
    - id            : identifiant complet (urn:ngsi-ld:EcoCounter:...)
    - serial_number : dernière partie de l'ID (X2H..., etc.)
    - vehicle_type  : 'bicycle', 'pedestrian', etc.
    - display_name  : nom du compteur (si disponible)
    - lat           : latitude
    - lon           : longitude

    Utilisation typique :
    - Récupérer ce CSV pour alimenter un script ETL (par ex. ETL/etl_bike_data.py)
    qui téléchargera ensuite les séries temporelles détaillées.
    """
    df = fetch_all_counters_flat()

    if vehicle_type is not None:
        df = df[df["vehicle_type"] == vehicle_type].reset_index(drop=True)

    if df.empty:
        raise HTTPException(
            status_code=404, detail="Aucun compteur trouvé avec ce filtre."
        )

    if format == "csv":
        return df_to_csv_response(df, "bike_geoloc_counters.csv")

    # format json
    return JSONResponse(df_to_json_records(df))

