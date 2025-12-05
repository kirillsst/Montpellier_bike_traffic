# train_model/saver.py
from prophet.serialize import model_to_json
from config import ARTIFACTS_DIR
import pandas as pd

def save_model(model, counter_name):
    """Sauvegarde le modèle Prophet en JSON."""
    safe_name = counter_name.replace(" ", "_").replace("/", "-")
    filename = ARTIFACTS_DIR / f"prophet_{safe_name}.json"
    
    with open(filename, 'w') as fout:
        fout.write(model_to_json(model))
    
    return filename.name

def save_metrics(results_list):
    """Sauvegarde le tableau récapitulatif des scores."""
    df_res = pd.DataFrame(results_list).sort_values("MAE")
    path = ARTIFACTS_DIR / "training_metrics.csv"
    df_res.to_csv(path, index=False)
    print(f"\n✅ Métriques sauvegardées dans : {path}")
    return df_res