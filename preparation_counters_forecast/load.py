from .config import get_supabase_client

def upload_forecast_data(df, target_table="counters_forecast"):
    """Envoie les données préparées vers Supabase."""
    supabase = get_supabase_client()
    records = df.to_dict(orient="records")
    total = len(records)
    
    print(f"   [Load] Envoi de {total} lignes vers '{target_table}'...")
    
    # Note : Idéalement, on supprimerait les anciennes données ici si nécessaire
    # supabase.table(target_table).delete().neq("id", 0).execute() 
    # (Attention, delete all sur supabase demande souvent une clause where)

    batch_size = 100
    for i in range(0, total, batch_size):
        batch = records[i:i+batch_size]
        try:
            supabase.table(target_table).insert(batch).execute()
        except Exception as e:
            print(f"   ❌ Erreur sur le bloc {i}: {e}")
            
    print("   [Load] Chargement terminé.")