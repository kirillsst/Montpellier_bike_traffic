import pandas as pd
import json
import os
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client
from datetime import datetime

# --- CONFIGURATION ---
BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"

load_dotenv(ENV_PATH)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TABLE_NAME = "predictions_hourly"

OUTPUT_DIR = BASE_DIR / "frontend" / "assets" / "data"
OUTPUT_FILE = OUTPUT_DIR / "dashboard_data.json"

# --- FONCTION DE FORMATAGE DATE (FR) ---
def format_date_fr(dt_obj):
    """Transforme un objet datetime en 'Lundi 01 Décembre 2025'"""
    days = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]
    months = ["", "Janvier", "Février", "Mars", "Avril", "Mai", "Juin", 
              "Juillet", "Août", "Septembre", "Octobre", "Novembre", "Décembre"]
    
    weekday = days[dt_obj.weekday()]
    month = months[dt_obj.month]
    
    return f"{weekday} {dt_obj.day:02d} {month} {dt_obj.year}"

# --- 1. CONNEXION SUPABASE ---
print(f"🌍 Connexion à Supabase...")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ Erreur : SUPABASE_URL ou SUPABASE_KEY manquant.")
    exit(1)

try:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    response = supabase.table(TABLE_NAME).select("*").execute()
    rows = response.data
    
    if not rows:
        print("⚠️ La table est vide !")
        exit(1)

    df = pd.DataFrame(rows)
    print(f"✅ Données récupérées : {len(df)} lignes")

except Exception as e:
    print(f"❌ Erreur Supabase : {e}")
    exit(1)

# --- 2. TRAITEMENT & CORRECTION DATE ---
counters_data = []

# Harmonisation des colonnes
if 'predicted_intensity' not in df.columns and 'intensity' in df.columns:
    df.rename(columns={'intensity': 'predicted_intensity'}, inplace=True)

# --- CORRECTION DATE ICI ---
if 'date' in df.columns:
    # dayfirst=True force la lecture JJ/MM/AAAA au lieu de MM/JJ/AAAA
    df['date'] = pd.to_datetime(df['date'], dayfirst=True)
    
    # DEBUG : Afficher les dates trouvées
    unique_dates = df['date'].dt.date.unique()
    print(f"📅 Dates trouvées dans la base : {unique_dates}")

    # On prend la date la plus récente
    target_date = df['date'].max()
    df = df[df['date'] == target_date]
    print(f"🎯 Date sélectionnée pour le dashboard : {target_date.date()}")

for name, group in df.groupby('name'):
    group = group.sort_values('hour')
    total_traffic = group['predicted_intensity'].sum()
    
    lat = group['latitude'].iloc[0] if 'latitude' in group and pd.notna(group['latitude'].iloc[0]) else 43.6107
    lon = group['longitude'].iloc[0] if 'longitude' in group and pd.notna(group['longitude'].iloc[0]) else 3.8767

    counters_data.append({
        "name": name,
        "lat": float(lat),
        "lon": float(lon),
        "total": int(total_traffic),
        "hourly": group['predicted_intensity'].fillna(0).astype(int).tolist()
    })

counters_data.sort(key=lambda x: x['total'], reverse=True)
counters_data = counters_data[:10]

# --- 3. COULEURS ---
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

# --- 4. EXPORT JSON ---
# On utilise la date filtrée du DataFrame
date_str = "Date Inconnue"
if not df.empty and 'date' in df.columns:
    date_str = format_date_fr(df['date'].iloc[0])

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

try:
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump({
            "meta": {"date": date_str},
            "data": counters_data
        }, f, indent=4)
    
    print(f"✅ SUCCÈS : JSON mis à jour. Date affichée : {date_str}")

except Exception as e:
    print(f"❌ Erreur écriture JSON : {e}")