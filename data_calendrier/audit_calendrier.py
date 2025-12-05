import pandas as pd
from pathlib import Path

# --- CONFIGURATION DU CHEMIN EXACT ---
FILE_PATH = Path("/Montpellier_bike_traffic/data_calendrier/data/calendrier_complet.csv")

def audit():
    print(f"--- AUDIT DE QUALITÉ : {FILE_PATH} ---")
    
    if not FILE_PATH.exists():
        print("❌ Erreur : Le fichier est introuvable à l'emplacement spécifié.")
        return

    # 1. Chargement
    df = pd.read_csv(FILE_PATH)
    df['date'] = pd.to_datetime(df['date'])
    
    # 2. Vérification Temporelle (Continuité)
    start_date = df['date'].min()
    end_date = df['date'].max()
    
    # On génère la grille théorique parfaite (2023 -> 2026 inclus)
    expected_range = pd.date_range(start="2023-01-01", end="2026-12-31", freq='D')
    nb_jours_attendus = len(expected_range) # 1461 jours (avec 2024 bissextile)
    
    print("\n1. Période couverte :")
    print(f"   Du {start_date.date()} au {end_date.date()}")
    print(f"   Lignes réelles   : {len(df)}")
    print(f"   Lignes attendues : {nb_jours_attendus}")
    
    if len(df) == nb_jours_attendus:
        print("   ✅ PARFAIT : Aucune journée manquante sur 4 ans.")
    else:
        # On identifie les trous
        missing = expected_range.difference(df['date'])
        print(f"   ❌ ERREUR : Il manque {len(missing)} jours !")
        if len(missing) < 10:
            print(f"   Jours manquants : {missing.date}")

    # 3. Vérification des Valeurs Vides (NaN)
    print("\n2. Valeurs vides (NaN) :")
    nb_na = df.isna().sum().sum()
    if nb_na == 0:
        print("   ✅ PARFAIT : Le tableau est 100% rempli.")
    else:
        print(f"   ⚠️ ATTENTION : Il y a {nb_na} valeurs vides.")
        print(df.isna().sum())

    # 4. Vérification des Colonnes
    print("\n3. Colonnes présentes :")
    cols_attendues = ["date", "jour_semaine", "is_weekend", "nom_jour", "is_ferie", "is_vacances", "is_jour_ouvre"]
    
    # On vérifie l'ordre et la présence
    if list(df.columns) == cols_attendues:
        print(f"   ✅ PARFAIT : {list(df.columns)}")
    else:
        print(f"   ⚠️ DIFFÉRENCE : {list(df.columns)}")
        print(f"   Attendu      : {cols_attendues}")

    # 5. Test de cohérence (Logique métier)
    # Un jour ne peut pas être ouvré ET (Week-end OU Férié)
    print("\n4. Test de cohérence logique (Jour Ouvré) :")
    incoherent = df[ (df['is_jour_ouvre'] == 1) & ((df['is_weekend'] == 1) | (df['is_ferie'] == 1)) ]
    
    if incoherent.empty:
        print("   ✅ Logique respectée : Aucun jour ouvré n'est férié ou week-end.")
    else:
        print(f"   ❌ ERREUR DE LOGIQUE : {len(incoherent)} lignes incohérentes trouvées.")

if __name__ == "__main__":
    audit()