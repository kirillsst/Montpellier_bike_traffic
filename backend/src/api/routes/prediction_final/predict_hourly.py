import pandas as pd
import joblib
from train_model_xgboost import config, loader
from src.api.utils.supabase_client import supabase

INPUT_TABLE = "counters_forecast"
OUTPUT_TABLE = "predictions_hourly"


# -------------------------
# Fetch all rows with pagination
# -------------------------
def fetch_all(table_name: str) -> pd.DataFrame:
    all_rows = []
    offset = 0
    limit = 1000

    while True:
        resp = supabase.table(table_name).select("*").range(offset, offset + limit - 1).execute()
        rows = resp.data

        if not rows:
            break

        all_rows.extend(rows)
        offset += limit

    return pd.DataFrame(all_rows)


# -------------------------
# Overwrite table
# -------------------------
def overwrite_table(table_name: str, data: list):
    """
    Completely clears a Supabase table and inserts new data.
    """

    print(f"[INFO] Clearing table '{table_name}'...")

    try:
        supabase.table(table_name).delete().neq("id", 0).execute()
        print(f"[INFO] Table '{table_name}' cleared.")
    except Exception as e:
        print(f"[ERROR] Failed to clear table '{table_name}': {e}")
        return

    if not data:
        print("[WARNING] No new data to insert.")
        return

    print(f"[INFO] Inserting {len(data)} new rows...")

    try:
        batch_size = 100
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            supabase.table(table_name).insert(batch).execute()
            print(f"   -> Batch {i}-{i + len(batch)} inserted")
    except Exception as e:
        print(f"[ERROR] Failed to insert new rows: {e}")
        return

    print(f"[SUCCESS] Table '{table_name}' overwritten successfully.")


# -------------------------
# Main prediction function
# -------------------------
def run_prediction_pipeline(target_date: str = None):
    print(f"🚀 Starting prediction from '{INPUT_TABLE}'...")

    # Load data
    df_day = fetch_all(INPUT_TABLE)

    if df_day.empty:
        print(f"⚠️ Table {INPUT_TABLE} is empty.")
        return

    # Preprocess timestamps
    df_day['timestamp'] = pd.to_datetime(df_day['timestamp'])
    df_day = df_day.sort_values(by=['name', 'timestamp'])

    # Determine target date
    if target_date:
        target_date = pd.to_datetime(target_date).date()
    else:
        target_date = df_day['timestamp'].dt.date.iloc[0]

    target_date_str = str(target_date)
    print(f"📅 Target date: {target_date_str}")

    # Filter rows for required date
    df_day = df_day[df_day['timestamp'].dt.date == target_date]

    if df_day.empty:
        print(f"⚠️ No data for {target_date_str}")
        return

    # Feature engineering
    df_day['hour'] = df_day['timestamp'].dt.hour
    df_day['dayofyear'] = df_day['timestamp'].dt.dayofyear
    df_day['month'] = df_day['timestamp'].dt.month
    df_day['year'] = df_day['timestamp'].dt.year
    df_day['dayofweek'] = df_day['timestamp'].dt.dayofweek

    predictions_list = []
    counters = df_day['name'].unique()

    print(f"🤖 Loading models from: {config.ARTIFACTS_DIR}")

    # Predict for each counter
    for name in counters:
        df_c = df_day[df_day['name'] == name].copy()

        lat = df_c['latitude'].iloc[0]
        lon = df_c['longitude'].iloc[0]

        safe_name = name.replace(" ", "_").replace("/", "-")
        model_path = config.ARTIFACTS_DIR / f"xgboost_{safe_name}.joblib"

        if not model_path.exists():
            print(f"[WARNING] Model missing for: {name}")
            continue

        model = joblib.load(model_path)

        try:
            X = df_c[loader.FEATURES_XGBOOST]
            preds = model.predict(X)
            y_pred = [int(max(0, p)) for p in preds]

            for i, val in enumerate(y_pred):
                predictions_list.append({
                    "name": name,
                    "date": target_date_str,
                    "hour": int(df_c['hour'].iloc[i]),
                    "predicted_intensity": val,
                    "latitude": lat,
                    "longitude": lon
                })
        except KeyError as e:
            print(f"[ERROR] Missing column for {name}: {e}")
            print("Available:", df_c.columns.tolist())
            return

    if not predictions_list:
        print("❌ No predictions generated.")
        return

    # Overwrite table
    print(f"☁️ Overwriting Supabase table '{OUTPUT_TABLE}'...")
    overwrite_table(OUTPUT_TABLE, predictions_list)

    print("✅ Prediction pipeline finished successfully!")
    return predictions_list

if __name__ == "__main__":
    run_prediction_pipeline()
