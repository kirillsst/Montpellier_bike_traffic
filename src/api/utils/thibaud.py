import requests
import pandas as pd
import datetime
from tqdm import tqdm
import os
import numpy as np

# --- CONFIGURATION ---
BASE_URL = "https://portail-api-data.montpellier3m.fr"
# Date de fin souhaitée
DATE_FIN_CIBLE = "2025-11-29T23:59:59"

# Chemin vers votre fichier local
PATH_GEO_CSV = "/home/thibaud/Montpellier_bike_traffic/exploration/Géoloc_Comtpeurs.csv"

# ---------------------------------------------------------------------------
# 1. Chargement et Restructuration du CSV LOCAL
# ---------------------------------------------------------------------------
def load_and_pivot_local_csv(filepath):
    if not os.path.exists(filepath):
        print(f"ERREUR : Le fichier {filepath} est introuvable.")
        return pd.DataFrame()

    try:
        df_raw = pd.read_csv(filepath, sep=';', encoding='latin1')
        df_raw = df_raw.iloc[:, [0, 1, 2, 3, 4]]
        df_raw.columns = ["nom_csv", "serial_a", "serial_b", "latitude", "longitude"]

        for col in ["latitude", "longitude"]:
            df_raw[col] = df_raw[col].astype(str).str.replace(',', '.', regex=False)
            df_raw[col] = pd.to_numeric(df_raw[col], errors='coerce')

        df_part_a = df_raw[["nom_csv", "serial_a", "latitude", "longitude"]].copy()
        df_part_a = df_part_a.rename(columns={"serial_a": "serial_number"})
        
        df_part_b = df_raw[["nom_csv", "serial_b", "latitude", "longitude"]].copy()
        df_part_b = df_part_b.rename(columns={"serial_b": "serial_number"})
        
        df_combined = pd.concat([df_part_a, df_part_b], ignore_index=True)
        
        df_combined = df_combined.dropna(subset=["serial_number"])
        df_combined["serial_number"] = df_combined["serial_number"].astype(str).str.strip()
        df_combined = df_combined[df_combined["serial_number"] != "nan"]
        df_combined = df_combined.drop_duplicates(subset=["serial_number"])
        
        print(f"Référentiel local chargé : {len(df_combined)} numéros de série uniques trouvés.")
        return df_combined

    except Exception as e:
        print(f"Erreur lecture CSV local : {e}")
        return pd.DataFrame()

# ---------------------------------------------------------------------------
# 2. Récupération API
# ---------------------------------------------------------------------------
def fetch_api_counters_list():
    url = f"{BASE_URL}/ecocounter"
    limit = 1000 # On laisse à 1000 pour éviter le crash
    all_results = []
    offset = 0
    
    print("Appel API pour récupérer les IDs...")
    while True:
        try:
            full_url = f"{url}?limit={limit}&offset={offset}"
            r = requests.get(full_url)
            r.raise_for_status()
            data = r.json()
            
            if not isinstance(data, list) or not data: 
                break
            
            all_results.extend(data)
            
            if len(data) < limit:
                break
                
            offset += limit
        except Exception as e:
            print(f"Erreur API liste : {e}")
            break
            
    df = pd.DataFrame(all_results)
    
    if df.empty:
        print("⚠️ ERREUR : API vide.")
        return pd.DataFrame()

    if "id" not in df.columns:
        return pd.DataFrame()

    df["serial_number"] = df["id"].apply(lambda x: str(x).split(':')[-1].strip() if x else "")
    return df

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    
    # 1. & 2. Chargement
    df_geo = load_and_pivot_local_csv(PATH_GEO_CSV)
    df_api = fetch_api_counters_list()
    
    if df_geo.empty or df_api.empty: 
        print("Arrêt : Données manquantes.")
        exit()

    # 3. Fusion
    print("Fusion des données API avec le CSV local...")
    df_merged = pd.merge(df_api, df_geo, on="serial_number", how="inner")
    print(f"Compteurs correspondants trouvés : {len(df_merged)}")

    # 4. Téléchargement PAR MORCEAUX (Chunking)
    all_dfs = []
    
    # On définit les périodes année par année pour éviter la surcharge API
    PERIODES = [
        ("2023-01-01T00:00:00", "2023-12-31T23:59:59"),
        ("2024-01-01T00:00:00", "2024-12-31T23:59:59"),
        ("2025-01-01T00:00:00", DATE_FIN_CIBLE) # S'arrête à la date voulue
    ]
    
    print(f"Téléchargement par année (2023, 2024, 2025)...")
    
    for index, row in tqdm(df_merged.iterrows(), total=len(df_merged)):
        api_id = row["id"]
        
        # BOUCLE INTERNE : On fait 3 appels par compteur au lieu d'un seul
        for start_date, end_date in PERIODES:
            
            params = {
                "fromDate": start_date, 
                "toDate": end_date
            }
            
            try:
                r = requests.get(f"{BASE_URL}/ecocounter_timeseries/{api_id}/attrs/intensity", params=params)
                
                if r.status_code == 200:
                    ts = r.json()
                    # Petite verif de sécurité
                    if isinstance(ts, dict) and "index" in ts and "values" in ts and len(ts["index"]) > 0:
                        
                        df_temp = pd.DataFrame({
                            "timestamp": pd.to_datetime(ts["index"]),
                            "intensity": ts["values"],
                            "name": row["nom_csv"],       
                            "latitude": row["latitude"],
                            "longitude": row["longitude"]
                        })
                        all_dfs.append(df_temp)
            except:
                # En cas d'erreur sur une année, on continue les autres
                pass

    # 5. SAUVEGARDE ET CONSOLIDATION
    if all_dfs:
        df_concat = pd.concat(all_dfs, ignore_index=True)
        
        print(f"Lignes brutes récupérées : {len(df_concat)}")
        print("--- Consolidation finale ---")
        
        # GroupBy pour recoller les morceaux des années et des IDs
        df_final = df_concat.groupby(['name', 'latitude', 'longitude', 'timestamp'], as_index=False)['intensity'].sum()
        df_final = df_final.sort_values(by=["name", "timestamp"])
        
        # Filtre de sécurité final pour être sûr de ne pas dépasser la date cible
        # (au cas où l'API renvoie un peu plus large)
        df_final = df_final[df_final["timestamp"] <= DATE_FIN_CIBLE]
        
        print(f"Lignes après consolidation : {len(df_final)}")
        print("--- RÉSULTAT FINAL ---")
        print(df_final.info())
        print(f"Date Min : {df_final['timestamp'].min()}")
        print(f"Date Max : {df_final['timestamp'].max()}")
        
        filename = "montpellier_velo_final"
        df_final.to_parquet(f"{filename}.parquet", index=False)
        df_final.to_csv(f"{filename}.csv", index=False)
        print(f"✅ Fichiers sauvegardés : {filename}.parquet")
    else:
        print("Aucune donnée d'historique récupérée.")