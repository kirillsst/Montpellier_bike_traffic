import argparse
from datetime import datetime, timedelta
# Imports relatifs (nécessite d'exécuter en tant que module ou d'ajuster le path)
from preparation_counters_forecast import extract, transform, load

def main(target_date):
    print(f"🚀 Démarrage du pipeline ETL pour le : {target_date}")
    
    # 1. EXTRACT
    df_counters = extract.get_unique_counters()
    df_meteo = extract.get_meteo_forecast(target_date)
    calendar_info = extract.get_calendar_info(target_date)
    
    # 2. TRANSFORM
    df_final = transform.build_forecast_dataset(df_counters, df_meteo, calendar_info, target_date)
    
    print(f"   📊 Données transformées : {len(df_final)} lignes générées.")
    
    # 3. LOAD
    load.upload_forecast_data(df_final)
    
    print("✅ Pipeline terminé avec succès.")

if __name__ == "__main__":
    # Par défaut J+1, ou date passée en argument
    default_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    
    # Gestion simple d'argument pour tester d'autres dates
    # Ex: python -m data_preparation.run_pipeline --date 2025-12-11
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default="2025-12-11", help="Date cible YYYY-MM-DD")
    args = parser.parse_args()
    
    main(args.date)