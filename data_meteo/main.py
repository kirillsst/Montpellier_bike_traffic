# data_meteo/main.py
from pathlib import Path
from pipeline import MeteoPipeline

if __name__ == "__main__":
    # 1. Définition du chemin dossier "data" DANS "data_meteo"
    # __file__ = le chemin de ce script (data_meteo/main.py)
    # .parent  = le dossier data_meteo/
    # / "data" = data_meteo/data/
    target_dir = Path(__file__).resolve().parent / "data"
    
    print(f"📂 Les données seront stockées dans : {target_dir}")

    # 2. Lancement du Pipeline avec ce chemin cible
    pipeline = MeteoPipeline(base_dir=str(target_dir))
    
    # 3. Exécution
    res = pipeline.run()
    
    if res:
        print("\n✨ Fichiers disponibles :")
        for k, v in res.items():
            print(f" - {k} : {v}")