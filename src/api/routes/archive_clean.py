from fastapi import APIRouter, HTTPException
from src.api.utils.supabase_client import supabase
import pandas as pd
from tqdm import tqdm

router = APIRouter()

@router.post("/process_top10")
def process_top10():
    try:
        print("\n=== 1️⃣ DOWNLOAD FROM SUPABASE: counters ===")

        all_rows = []
        page_size = 10000
        last_timestamp = None  # for keyset-pagination

        while True:
            query = supabase.table("counters").select("*").order("timestamp")
            if last_timestamp:
                query = query.gt("timestamp", last_timestamp)

            resp = query.limit(page_size).execute()
            batch = resp.data

            if not batch:
                break

            all_rows.extend(batch)
            last_timestamp = batch[-1]["timestamp"]  # last timestamp

        print(f"Rows downloaded: {len(all_rows)}")
        if not all_rows:
            raise HTTPException(status_code=404, detail="La table counters est vide")

        print("\n=== 2️⃣ CREATE DATAFRAME ===")
        df = pd.DataFrame(all_rows)
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

        print("Timestamp MIN:", df['timestamp'].min())
        print("Timestamp MAX:", df['timestamp'].max())

        print("\n=== 3️⃣ COMPUTE LAST MONTH ===")
        today = pd.Timestamp.now('UTC')
        last_month = (today - pd.DateOffset(months=1)).to_period('M')
        debut_mois_precedent = last_month.start_time.tz_localize('UTC')
        fin_mois_precedent = last_month.end_time.tz_localize('UTC')

        print("Last month start:", debut_mois_precedent)
        print("Last month end  :", fin_mois_precedent)

        mask_periode = (df['timestamp'] >= debut_mois_precedent) & (df['timestamp'] <= fin_mois_precedent)
        compteurs_actifs = df[mask_periode & (df['intensity'] > 0)]['name'].unique()

        print(f"Active counters in that month: {len(compteurs_actifs)}")
        print("Counters list:", compteurs_actifs.tolist())

        if len(compteurs_actifs) == 0:
            raise HTTPException(status_code=404, detail="Aucun compteur Top 10 trouvé")

        print("\n=== 4️⃣ HISTORICAL RELIABILITY ===")
        df['date'] = df['timestamp'].dt.date
        matrix = df.groupby(['name', 'date'])['intensity'].sum().unstack().fillna(0)

        print("Matrix shape:", matrix.shape)
        print("Matrix sample columns (dates):", list(matrix.columns)[:5], "...")

        jours_hs = (matrix == 0).sum(axis=1).reset_index()
        jours_hs.columns = ['name', 'nb_hs']
        jours_hs['taux_panne'] = (jours_hs['nb_hs'] / len(matrix.columns) * 100)
        df_selection = jours_hs[jours_hs['name'].isin(compteurs_actifs)]
        top_10_names = df_selection.sort_values(by='taux_panne', ascending=True).head(10)['name'].tolist()

        print("Top10:", top_10_names)

        print("\n=== 5️⃣ CREATE FULL HOURLY GRID ===")
        df_top = df[df['name'].isin(top_10_names)].copy()
        meta_coords = df_top[['name', 'latitude', 'longitude']].drop_duplicates(subset=['name'])

        full_time_range = pd.date_range(
            start=df['timestamp'].min(),
            end=df['timestamp'].max(),
            freq='h',
            name='timestamp'
        )

        print("full_time_range start:", full_time_range.min())
        print("full_time_range end  :", full_time_range.max())
        print("Len full_time_range:", len(full_time_range))

        df_dates = pd.DataFrame(full_time_range)
        df_names = pd.DataFrame({'name': top_10_names})
        df_grid = df_names.merge(df_dates, how='cross')

        print("df_grid rows:", len(df_grid))

        df_final = df_grid.merge(
            df_top[['timestamp', 'name', 'intensity']],
            on=['timestamp', 'name'],
            how='left'
        ).merge(meta_coords, on='name', how='left')

        print("df_final rows after merge:", len(df_final))
        print(df_final.head())

        print("\n=== 6️⃣ INTERPOLATION ===")
        df_final = df_final.sort_values(['name', 'timestamp'])
        df_final['intensity'] = df_final.groupby('name')['intensity'].transform(
            lambda g: g.interpolate(method='linear', limit_direction='both')
        )
        df_final['intensity'] = df_final['intensity'].fillna(0).round().astype(int)

        print("Missing intensities after interpolate:", df_final['intensity'].isna().sum())

        print("\n=== 7️⃣ UPLOAD TO SUPABASE ===")
        df_final_to_insert = df_final.copy()
        df_final_to_insert['timestamp'] = df_final_to_insert['timestamp'].apply(lambda x: x.isoformat())
        records = df_final_to_insert.to_dict(orient='records')

        print("Total rows to upload:", len(records))

        for i in tqdm(range(0, len(records), 500), desc="Uploading batches"):
            batch = records[i:i+500]
            supabase.table("counters_clean").insert(batch).execute()

        print("UPLOAD FINISHED ✔")

        return {
            "status": "success",
            "rows_uploaded": len(records),
            "top10_names": top_10_names,
            "period_start": debut_mois_precedent.strftime("%Y-%m-%d"),
            "period_end": fin_mois_precedent.strftime("%Y-%m-%d")
        }

    except Exception as e:
        print("\n❌ ERROR:", str(e))
        raise HTTPException(status_code=500, detail=str(e))
