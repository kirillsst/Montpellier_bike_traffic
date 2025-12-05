import pandas as pd
from pathlib import Path

# --- CONFIGURATION DES CHEMINS ---
BASE_DIR = Path("/home/thibaud/Montpellier_bike_traffic")

FILE_VELO = BASE_DIR / "exploration/top10_complet_interpolated.csv"
FILE_METEO = BASE_DIR / "data_meteo/data/clean/hourly_history.csv"
FILE_CALENDRIER = BASE_DIR / "data_calendrier/data/calendrier_complet.csv"

OUTPUT_FILE = BASE_DIR / "dataset_final_training.csv"

def main():
    print("--- Démarrage de l'agrégation finale ---")

    # 1. CHARGEMENT DES DONNÉES
    print("1. Chargement des CSV...")
    
    # A. Vélos (La base principale)
    df_velo = pd.read_csv(FILE_VELO)
    # Conversion UTC explicite pour éviter les conflits
    df_velo['timestamp'] = pd.to_datetime(df_velo['timestamp'], utc=True)
    print(f"   - Vélos      : {len(df_velo)} lignes (10 compteurs)")

    # B. Météo (Données horaires)
    df_meteo = pd.read_csv(FILE_METEO)
    df_meteo['timestamp'] = pd.to_datetime(df_meteo['time'], utc=True) # On harmonise le nom
    df_meteo = df_meteo.drop(columns=['time']) # On retire l'ancienne colonne
    print(f"   - Météo      : {len(df_meteo)} lignes")

    # C. Calendrier (Données journalières)
    df_cal = pd.read_csv(FILE_CALENDRIER)
    df_cal['date'] = pd.to_datetime(df_cal['date']) # Pas de UTC nécessaire pour une date simple
    print(f"   - Calendrier : {len(df_cal)} jours")

    # ---------------------------------------------------------
    # 2. FUSION VÉLOS + MÉTÉO (Jointure Horaire)
    # ---------------------------------------------------------
    print("\n2. Fusion Météo (Hourly join)...")
    
    # On fait un LEFT JOIN : on garde toutes les lignes vélo, on y colle la météo correspondante
    df_merged = pd.merge(df_velo, df_meteo, on='timestamp', how='left')
    
    # Vérification : A-t-on perdu des lignes ?
    if len(df_merged) != len(df_velo):
        print("⚠️ ALERTE : Le nombre de lignes a changé après la fusion météo !")
    else:
        print("   ✅ Fusion Météo OK.")

    # ---------------------------------------------------------
    # 3. FUSION + CALENDRIER (Jointure Journalière)
    # ---------------------------------------------------------
    print("3. Fusion Calendrier (Daily join)...")
    
    # Pour joindre le calendrier, il nous faut une colonne "date" (YYYY-MM-DD) dans le dataset vélo
    # Attention : .dt.normalize() garde le format datetime mais met l'heure à 00:00:00
    df_merged['date_join'] = df_merged['timestamp'].dt.tz_convert(None).dt.normalize()
    
    # Le fichier calendrier n'a pas de fuseau, on s'assure que c'est compatible
    df_cal['date'] = pd.to_datetime(df_cal['date'])
    
    # Fusion sur la colonne date
    df_final = pd.merge(df_merged, df_cal, left_on='date_join', right_on='date', how='left')
    
    # Nettoyage des colonnes techniques de jointure
    df_final = df_final.drop(columns=['date_join', 'date'])
    
    if len(df_final) != len(df_velo):
        print("⚠️ ALERTE : Problème lors de la fusion calendrier.")
    else:
        print("   ✅ Fusion Calendrier OK.")

    # ---------------------------------------------------------
    # 4. VÉRIFICATIONS ET NETTOYAGE FINAL
    # ---------------------------------------------------------
    print("\n4. Vérification de la qualité des données...")
    
    # Vérifier s'il y a des NaN (trous de jointure)
    nan_counts = df_final.isna().sum()
    if nan_counts.sum() > 0:
        print("⚠️ ATTENTION : Il reste des valeurs manquantes après fusion :")
        print(nan_counts[nan_counts > 0])
        
        # Optionnel : Remplissage simple des trous météo par interpolation (si quelques heures manquent)
        # df_final['temperature_2m'] = df_final['temperature_2m'].interpolate()
    else:
        print("   ✅ Dataset complet (aucun NaN).")

    # ---------------------------------------------------------
    # 5. SAUVEGARDE
    # ---------------------------------------------------------
    print(f"\n5. Sauvegarde...")
    df_final.to_csv(OUTPUT_FILE, index=False)
    
    print(f"✅ Fichier final généré : {OUTPUT_FILE}")
    print(f"   Structure : {df_final.shape} (Lignes, Colonnes)")
    print("   Colonnes  :", list(df_final.columns))

if __name__ == "__main__":
    main()