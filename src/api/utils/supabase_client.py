import os
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
try:
    response = supabase.table("counters").select("*").limit(1).execute()
    if response.data is not None:
        print("Connection work")
        print(response.data)
    else:
        print("Connexion établie, mais aucune donnée disponible.")
except Exception as e:
    print(e)
