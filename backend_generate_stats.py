import pandas as pd
import json
import os
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client

# --- CONFIG ---
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TABLE_NAME = "counters_final"

OUTPUT_DIR = BASE_DIR / "frontend" / "assets" / "data"
OUTPUT_FILE = OUTPUT_DIR / "stats_data.json"

def fetch_historical_data():
    print(f"🌍 Récupération de l'historique complet ({TABLE_NAME})...")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # On ne récupère que les colonnes utiles pour les stats (Optimisation)
    cols = "timestamp, intensity, name, is_raining, nom_jour, month"
    
    all_rows = []
    offset = 0
    limit = 10000 # Gros chunks pour aller vite
    
    while True:
        # Pagination
        response = supabase.table(TABLE_NAME).select("*").range(offset, offset + limit - 1).execute()
        rows = response.data
        if not rows: break
        all_rows.extend(rows)
        offset += limit
        print(f"   -> {offset} lignes...")
        
    df = pd.DataFrame(all_rows)
    print(f"✅ Dataset chargé : {len(df)} lignes")
    
    # Conversion types
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

def generate_stats():
    df = fetch_historical_data()
    
    stats = {}

    # 1. KPI GLOBAUX
    stats['kpi'] = {
        "total_bikes": int(df['intensity'].sum()),
        "total_days": df['timestamp'].dt.date.nunique(),
        "avg_daily": int(df.groupby(df['timestamp'].dt.date)['intensity'].sum().mean())
    }

    # 2. EVOLUTION MENSUELLE (Line Chart)
    # On groupe par Mois (YYYY-MM)
    df['period'] = df['timestamp'].dt.to_period('M').astype(str)
    monthly = df.groupby('period')['intensity'].sum().sort_index()
    stats['monthly'] = {
        "labels": monthly.index.tolist(),
        "data": monthly.values.tolist()
    }

    # 3. MOYENNE PAR JOUR DE SEMAINE (Bar Chart)
    # On ordonne les jours proprement
    order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    df['nom_jour'] = pd.Categorical(df['nom_jour'], categories=order, ordered=True)
    weekly = df.groupby('nom_jour')['intensity'].mean() # Moyenne pour comparer WE vs Semaine
    stats['weekly'] = {
        "labels": ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"],
        "data": weekly.values.tolist()
    }

    # 4. IMPACT PLUIE (Doughnut)
    # Comparaison Moyenne horaire SANS pluie vs AVEC pluie
    rain_impact = df.groupby('is_raining')['intensity'].mean()
    stats['weather'] = {
        "labels": ["Temps Sec", "Pluie"],
        "data": [round(rain_impact.get(0, 0), 1), round(rain_impact.get(1, 0), 1)]
    }

    # Export
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=4)
    print(f"✅ Stats générées : {OUTPUT_FILE}")

if __name__ == "__main__":
    generate_stats()