# train_model_xgboost/saver.py
import joblib
import pandas as pd
from train_model_xgboost.config import ARTIFACTS_DIR

def save_model(model, counter_name):
    """Sauvegarde le modèle XGBoost."""
    safe_name = counter_name.replace(" ", "_").replace("/", "-")
    filename = ARTIFACTS_DIR / f"xgboost_{safe_name}.joblib"
    
    joblib.dump(model, filename)
    return filename.name

def save_metrics(results_list):
    df_res = pd.DataFrame(results_list).sort_values("MAE")
    path = ARTIFACTS_DIR / "training_metrics_xgboost.csv"
    df_res.to_csv(path, index=False)
    print(f"\n✅ Métriques XGBoost sauvegardées : {path}")