from fastapi import APIRouter
from src.api.utils.supabase_client import supabase
import pandas as pd
from tqdm import tqdm

router = APIRouter()

@router.post("/process_top10")
def process_top10():
    all_rows = []
    limit = 10000  # Taille du chunk pour récupérer les données en lot
    offset = 0

    # ===============================
    # 1. Récupération de toutes les données de la table counters
    # ===============================
    while True:
        resp = (
            supabase.table("counters")
            .select("*")
            .order("timestamp")
            .range(offset, offset + limit - 1)  # Pagination
            .execute()
        )
        data = resp.data
        if not data:
            break
        all_rows.extend(data)
        offset += limit

    if not all_rows:
        return {"status": "error", "message": "La table counters est vide"}

    # ===============================
    # 2. Création du DataFrame
    # ===============================
    df = pd.DataFrame(all_rows)
    if df.empty:
        return {"status": "error", "message": "Pas de données après conversion en DataFrame"}

    # Conversion de timestamp en datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Ajout de colonnes pour le jour de la semaine et l'heure
    # df['day_of_week'] = df['timestamp'].dt.weekday  # 0=Lundi, 6=Dimanche
    # df['hour_of_day'] = df['timestamp'].dt.hour

    # ===============================
    # 3. Sélection des Top10 compteurs
    # ===============================
    today = pd.Timestamp.now('UTC')
    fin_mois_precedent = (today.replace(day=1) - pd.Timedelta(days=1)).replace(hour=23, minute=59, second=59)
    debut_mois_precedent = fin_mois_precedent.replace(day=1, hour=0, minute=0, second=0)

    # Filtre des compteurs actifs sur le mois précédent
    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize('UTC')
    mask_periode = (df['timestamp'] >= debut_mois_precedent) & (df['timestamp'] <= fin_mois_precedent)
    compteurs_actifs = df[mask_periode & (df['intensity'] > 0)]['name'].unique()

    if len(compteurs_actifs) == 0:
        return {"status": "error", "message": "Aucun compteur Top 10 trouvé"}

    # Calcul de la fiabilité historique
    df['date'] = df['timestamp'].dt.date
    matrix = df.groupby(['name', 'date'])['intensity'].sum().unstack().fillna(0)
    jours_hs = (matrix == 0).sum(axis=1).reset_index()
    jours_hs.columns = ['name', 'nb_hs']
    jours_hs['taux_panne'] = (jours_hs['nb_hs'] / len(matrix.columns) * 100)
    df_selection = jours_hs[jours_hs['name'].isin(compteurs_actifs)]
    top_10_names = df_selection.sort_values(by='taux_panne', ascending=True).head(10)['name'].tolist()

    # ===============================
    # 4. Création de la grille complète pour les Top10
    # ===============================
    df_top = df[df['name'].isin(top_10_names)].copy()
    meta_coords = df_top[['name', 'latitude', 'longitude']].drop_duplicates(subset=['name'])

    # Création de la grille horaire complète
    full_time_range = pd.date_range(start=df['timestamp'].min(), end=df['timestamp'].max(), freq='h', name='timestamp')
    df_dates = pd.DataFrame(full_time_range)
    df_names = pd.DataFrame({'name': top_10_names})
    df_grid = df_names.merge(df_dates, how='cross')

    # Fusion des données réelles avec la grille
    df_final = pd.merge(df_grid, df_top[['timestamp', 'name', 'intensity']], on=['timestamp', 'name'], how='left')
    df_final = pd.merge(df_final, meta_coords, on='name', how='left')

    # ===============================
    # 5. Interpolation des valeurs manquantes
    # ===============================
    df_final = df_final.sort_values(by=['name', 'timestamp'])
    df_final['intensity'] = df_final.groupby('name')['intensity'].transform(
        lambda g: g.interpolate(method='linear', limit_direction='both')
    )
    df_final['intensity'] = df_final['intensity'].fillna(0).round().astype(int)

    # Recalcul du jour de la semaine et de l'heure (pour la grille complète)
    # df_final['day_of_week'] = df_final['timestamp'].dt.weekday
    # df_final['hour_of_day'] = df_final['timestamp'].dt.hour

    # ===============================
    # 6. Insertion dans la table counters_clean (batch de 500)
    # ===============================

    #convert to iso (kirillsst)
    # records = df_final.to_dict(orient="records")
    # for i in range(0, len(records), 500):
    #     batch = records[i:i+500]
    #     supabase.table("counters_clean").insert(batch).execute()

    # return {
    #     "status": "success",
    #     "rows_uploaded": len(records),
    #     "top10_names": top_10_names
    # }
    df_final_to_insert = df_final.copy()
    df_final_to_insert['timestamp'] = df_final_to_insert['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S')

    records = df_final_to_insert.to_dict(orient="records")

    for i in range(0, len(records), 500):
        batch = records[i:i+500]
        supabase.table("counters_clean").insert(batch).execute()

    min_ts = df_final_to_insert['timestamp'].min()
    max_ts = df_final_to_insert['timestamp'].max()

    return {
        "status": "success",
        "rows_uploaded": len(records),
        "top10_names": top_10_names,
        "min_timestamp": min_ts,
        "max_timestamp": max_ts
    }
