# train_model/config.py
from pathlib import Path

# Chemins
BASE_DIR = Path(__file__).resolve().parent.parent
DATASET_PATH = BASE_DIR / "dataset_final_training.csv"
ARTIFACTS_DIR = BASE_DIR / "train_model" / "artifacts"

# Paramètres
CUTOFF_DATE = "2025-11-17" # Date de séparation Train / Test

# Création automatique du dossier de sortie
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)