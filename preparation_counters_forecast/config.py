import os
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client

# Chargement du .env depuis la racine du projet (un niveau au-dessus)
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

def get_supabase_client():
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Erreur : SUPABASE_URL ou SUPABASE_KEY manquant dans le .env")
    return create_client(SUPABASE_URL, SUPABASE_KEY)