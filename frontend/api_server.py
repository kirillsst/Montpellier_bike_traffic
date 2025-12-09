from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import pandas as pd
import os
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client

# --- CONFIGURATION ---
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

app = FastAPI()

def get_supabase():
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise HTTPException(500, "Supabase credentials manquants dans le .env")
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def format_date_fr(dt_obj):
    days = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]
    months = ["", "Janvier", "Février", "Mars", "Avril", "Mai", "Juin", 
              "Juillet", "Août", "Septembre", "Octobre", "Novembre", "Décembre"]
    return f"{days[dt_obj.weekday()]} {dt_obj.day:02d} {months[dt_obj.month]} {dt_obj.year}"

# ==============================================================================
# 1. API DASHBOARD
# ==============================================================================
@app.get("/api/dashboard-data")
def api_dashboard():
    try:
        supabase = get_supabase()
        response = supabase.table("predictions_hourly").select("*").execute()
        df = pd.DataFrame(response.data)
        
        if df.empty:
            return {"meta": {"date": "Aucune donnée"}, "data": []}

        date_str = "Date inconnue"
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], dayfirst=True)
            target_date = df['date'].max()
            df = df[df['date'] == target_date]
            date_str = format_date_fr(target_date)

        counters_data = []
        col_intensity = 'predicted_intensity' if 'predicted_intensity' in df.columns else 'intensity'

        for name, group in df.groupby('name'):
            group = group.sort_values('hour')
            total = group[col_intensity].sum()
            
            lat = group['latitude'].iloc[0] if 'latitude' in group and pd.notna(group['latitude'].iloc[0]) else 43.6107
            lon = group['longitude'].iloc[0] if 'longitude' in group and pd.notna(group['longitude'].iloc[0]) else 3.8767

            counters_data.append({
                "name": name,
                "lat": float(lat),
                "lon": float(lon),
                "total": int(total),
                "hourly": group[col_intensity].fillna(0).astype(int).tolist()
            })

        counters_data.sort(key=lambda x: x['total'], reverse=True)
        counters_data = counters_data[:10]
        
        if counters_data:
            vals = [c['total'] for c in counters_data]
            series = pd.Series(vals)
            q25, q50, q75 = series.quantile([0.25, 0.50, 0.75])
            
            def get_color(val):
                if val < q25: return '#27ae60'
                elif val < q50: return '#2980b9'
                elif val < q75: return '#f39c12'
                else: return '#c0392b'

            for c in counters_data:
                c['color'] = get_color(c['total'])
                c['formatted_total'] = f"{c['total']:,}".replace(",", " ")

        return {
            "meta": {"date": date_str},
            "data": counters_data
        }

    except Exception as e:
        print(f"Erreur API Dashboard: {e}")
        raise HTTPException(500, str(e))

# ==============================================================================
# 2. API STATS (CORRIGÉE : PAGINATION SÛRE)
# ==============================================================================
@app.get("/api/stats-data")
def api_stats():
    """Récupère TOUT l'historique sans sauter de lignes"""
    try:
        supabase = get_supabase()
        
        all_rows = []
        offset = 0
        limit = 1000 # <-- CORRECTION : On respecte la limite standard de Supabase
        
        cols_needed = "timestamp, intensity, name, is_raining, nom_jour"
        
        # Boucle de récupération complète
        while True:
            response = supabase.table("counters_final")\
                .select(cols_needed)\
                .range(offset, offset + limit - 1)\
                .execute()
            
            rows = response.data
            if not rows:
                break
                
            all_rows.extend(rows)
            offset += limit
            
            # Sécurité : Si on reçoit moins que la limite, c'est la fin
            if len(rows) < limit:
                break
            
        df = pd.DataFrame(all_rows)
        
        if df.empty:
            return {"kpi": {}, "monthly": {}, "weekly": {}, "weather": {}}

        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Calculs
        stats = {}
        stats['kpi'] = {
            "total_bikes": int(df['intensity'].sum()),
            "total_days": df['timestamp'].dt.date.nunique(),
            "avg_daily": int(df.groupby(df['timestamp'].dt.date)['intensity'].sum().mean())
        }

        df['period'] = df['timestamp'].dt.to_period('M').astype(str)
        monthly = df.groupby('period')['intensity'].sum().sort_index()
        stats['monthly'] = {
            "labels": monthly.index.tolist(),
            "data": monthly.values.tolist()
        }

        order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        weekly = df.groupby('nom_jour')['intensity'].mean().reindex(order)
        stats['weekly'] = {
            "labels": ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"],
            "data": weekly.fillna(0).values.tolist()
        }

        rain_impact = df.groupby('is_raining')['intensity'].mean()
        stats['weather'] = {
            "labels": ["Temps Sec", "Pluie"],
            "data": [round(rain_impact.get(0, 0), 1), round(rain_impact.get(1, 0), 1)]
        }

        return stats

    except Exception as e:
        print(f"Erreur API Stats: {e}")
        raise HTTPException(500, str(e))

# ==============================================================================
# 3. SERVITUDE DU SITE WEB
# ==============================================================================

app.mount("/assets", StaticFiles(directory=BASE_DIR / "assets"), name="assets")

@app.get("/")
async def read_index():
    index_path = BASE_DIR / "index.html"
    if not index_path.exists():
        raise HTTPException(404, "Fichier index.html introuvable")
    return FileResponse(index_path)