import pandas as pd
import joblib
from pathlib import Path

from train_model_xgboost import loader, config

def run_hourly_prediction(target_date: str) -> pd.DataFrame:
    """
    Pure function:
    - loads dataset
    - loads XGBoost models
    - generates hourly predictions
    - returns DataFrame ready for Supabase insertion
    """

    df = loader.load_full_dataset()

    target_dt = pd.to_datetime(target_date).date()
    df_day = df[df["timestamp"].dt.date == target_dt].copy()

    if df_day.empty:
        return pd.DataFrame()  # API will handle empty case

    df_day = loader.create_features(df_day)

    predictions_list = []
    compteurs = df_day["name"].unique()

    for name in compteurs:

        df_c = df_day[df_day["name"] == name].sort_values("timestamp")

        lat = df_c["latitude"].iloc[0]
        lon = df_c["longitude"].iloc[0]

        safe_name = name.replace(" ", "_").replace("/", "-")
        model_path = config.ARTIFACTS_DIR / f"xgboost_{safe_name}.joblib"

        if not model_path.exists():
            continue

        model = joblib.load(model_path)

        preds = model.predict(df_c[loader.FEATURES_XGBOOST])
        y_pred = [int(max(0, x)) for x in preds]

        for i, val in enumerate(y_pred):
            hour = df_c["hour"].iloc[i]

            predictions_list.append({
                "name": name,
                "date": target_date,
                "hour": hour,
                "predicted_intensity": val,
                "latitude": lat,
                "longitude": lon
            })

    return pd.DataFrame(predictions_list)
