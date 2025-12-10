from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import pandas as pd
import os
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv
from supabase import create_client
from datetime import datetime, timedelta

# --- CONFIGURATION ---
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

app = FastAPI(title="Bike Traffic Dashboard API")

# -----------------------------
# Helper Functions
# -----------------------------
def get_supabase():
    """Initialize and return Supabase client."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise HTTPException(500, "Supabase credentials are missing in .env")
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def format_date_fr(dt_obj: datetime) -> str:
    """Format a datetime object into French human-readable string."""
    days = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]
    months = ["", "Janvier", "Février", "Mars", "Avril", "Mai", "Juin",
              "Juillet", "Août", "Septembre", "Octobre", "Novembre", "Décembre"]
    return f"{days[dt_obj.weekday()]} {dt_obj.day:02d} {months[dt_obj.month]} {dt_obj.year}"

# -----------------------------
# Startup Event
# -----------------------------
@app.on_event("startup")
async def startup_refresh_predictions():
    """
    On application startup, automatically run the prediction pipeline for tomorrow (J+1)
    if there are no predictions in the table for that date.
    """
    from src.api.routes.prediction_final.predict_hourly import run_prediction_pipeline

    supabase = get_supabase()
    target_date = (datetime.utcnow() + timedelta(days=1)).strftime("%Y-%m-%d")

    try:
        # Check existing predictions
        response = supabase.table("predictions_hourly").select("*").eq("date", target_date).execute()
        if response.data:
            print(f"[STARTUP] Predictions for {target_date} already exist.")
            return

        print(f"[STARTUP] Running prediction pipeline for {target_date}...")
        predictions = run_prediction_pipeline(target_date=target_date)
        if not predictions:
            print(f"[STARTUP] No predictions generated for {target_date}.")
            return

        # Clear previous predictions and insert new ones
        supabase.table("predictions_hourly").delete().neq("id", -1).execute()
        supabase.table("predictions_hourly").insert(predictions).execute()
        print(f"[STARTUP] Predictions for {target_date} inserted successfully ({len(predictions)} rows).")

    except Exception as e:
        print(f"[STARTUP] Error while generating predictions: {e}")

# -----------------------------
# API Endpoint: Dashboard Data
# -----------------------------
@app.get("/api/dashboard-data")
def api_dashboard():
    """
    Returns top 10 bike counters with hourly predicted traffic.
    Automatically formats the date and assigns color codes based on total counts.
    """
    try:
        supabase = get_supabase()
        response = supabase.table("predictions_hourly").select("*").execute()
        df = pd.DataFrame(response.data)

        if df.empty:
            return {"meta": {"date": "No data"}, "data": []}

        # Get the latest date in the table
        df['date'] = pd.to_datetime(df['date'])
        target_date = df['date'].max()
        df = df[df['date'] == target_date]
        date_str = format_date_fr(target_date)

        # Prepare counters data
        counters_data = []
        col_intensity = 'predicted_intensity' if 'predicted_intensity' in df.columns else 'intensity'

        for name, group in df.groupby('name'):
            group = group.sort_values('hour')
            total = group[col_intensity].sum()
            lat = group['latitude'].iloc[0] if pd.notna(group['latitude'].iloc[0]) else 43.6107
            lon = group['longitude'].iloc[0] if pd.notna(group['longitude'].iloc[0]) else 3.8767

            counters_data.append({
                "name": name,
                "lat": float(lat),
                "lon": float(lon),
                "total": int(total),
                "hourly": group[col_intensity].fillna(0).astype(int).tolist()
            })

        # Sort top 10 counters
        counters_data.sort(key=lambda x: x['total'], reverse=True)
        counters_data = counters_data[:10]

        # Assign colors based on quantiles
        if counters_data:
            vals = [c['total'] for c in counters_data]
            series = pd.Series(vals)
            q25, q50, q75 = series.quantile([0.25, 0.5, 0.75])

            def get_color(val):
                if val < q25: return '#27ae60'
                elif val < q50: return '#2980b9'
                elif val < q75: return '#f39c12'
                else: return '#c0392b'

            for c in counters_data:
                c['color'] = get_color(c['total'])
                c['formatted_total'] = f"{c['total']:,}".replace(",", " ")

        return {"meta": {"date": date_str}, "data": counters_data}

    except Exception as e:
        raise HTTPException(500, str(e))

# -----------------------------
# API Endpoint: Stats Data
# -----------------------------
@app.get("/api/stats-data")
def api_stats():
    """
    Returns full historical stats of bike counters including:
    - KPI (total, avg daily)
    - Monthly totals
    - Weekly averages
    - Rain impact
    Pagination-safe to avoid missing rows from Supabase.
    """
    try:
        supabase = get_supabase()
        all_rows = []
        offset, limit = 0, 1000
        cols_needed = "timestamp, intensity, name, is_raining, nom_jour"

        while True:
            response = supabase.table("counters_final").select(cols_needed).range(offset, offset + limit - 1).execute()
            rows = response.data
            if not rows:
                break
            all_rows.extend(rows)
            offset += limit
            if len(rows) < limit:
                break

        df = pd.DataFrame(all_rows)
        if df.empty:
            return {"kpi": {}, "monthly": {}, "weekly": {}, "weather": {}}

        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # KPI
        stats = {
            "kpi": {
                "total_bikes": int(df['intensity'].sum()),
                "total_days": df['timestamp'].dt.date.nunique(),
                "avg_daily": int(df.groupby(df['timestamp'].dt.date)['intensity'].sum().mean())
            }
        }

        # Monthly totals
        df['period'] = df['timestamp'].dt.to_period('M').astype(str)
        monthly = df.groupby('period')['intensity'].sum().sort_index()
        stats['monthly'] = {"labels": monthly.index.tolist(), "data": monthly.values.tolist()}

        # Weekly averages
        order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        weekly = df.groupby('nom_jour')['intensity'].mean().reindex(order)
        stats['weekly'] = {"labels": ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"],
                           "data": weekly.fillna(0).values.tolist()}

        # Rain impact
        rain_impact = df.groupby('is_raining')['intensity'].mean()
        stats['weather'] = {
            "labels": ["Dry", "Rain"],
            "data": [round(rain_impact.get(0, 0), 1), round(rain_impact.get(1, 0), 1)]
        }

        return stats

    except Exception as e:
        raise HTTPException(500, str(e))

# -----------------------------
# Serve static frontend assets
# -----------------------------
app.mount("/assets", StaticFiles(directory=BASE_DIR / "assets"), name="assets")

@app.get("/")
async def read_index():
    """Serve the main frontend page."""
    index_path = BASE_DIR / "index.html"
    if not index_path.exists():
        raise HTTPException(404, "index.html not found")
    return FileResponse(index_path)
