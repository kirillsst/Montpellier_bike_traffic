# train_model_xgboost/config.py
import os
from pathlib import Path
from dotenv import load_dotenv

# Charge les variables du fichier .env situé à la racine
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

# --- CONFIGURATION SUPABASE ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

TABLE_NAME = "counters_final"

# Dossier pour les modèles (reste local pour l'instant)
ARTIFACTS_DIR = BASE_DIR / "train_model_xgboost" / "artifacts"
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

# Date de séparation (reste utile pour l'entrainement)
CUTOFF_DATE = "2025-11-30"