# data_meteo/main.py
from data_meteo.pipeline import MeteoPipeline

def run_pipeline():
    # On définit où on veut que tout se passe (dossier data)
    pipeline = MeteoPipeline(base_dir="data")
    
    # Lance tout le processus (Téléchargement -> Nettoyage -> Sauvegarde en DB)
    resultats = pipeline.run()
    
    # Petit check de fin
    if resultats:
        print("\n📊 Résumé des insertions dans la base :")
        print(f" - hourly_history : {resultats['hourly_history_rows']} lignes insérées")
        print(f" - hourly_forecast : {resultats['hourly_forecast_rows']} lignes insérées")
    
    return resultats

if __name__ == "__main__":
    run_pipeline()
