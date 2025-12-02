# main.py
from pipeline import MeteoPipeline

if __name__ == "__main__":
    # On définit où on veut que tout se passe (dossier data)
    pipeline = MeteoPipeline(base_dir="data")
    
    # Lance tout le processus (Téléchargement -> Nettoyage -> Sauvegarde)
    resultats = pipeline.run()
    
    # Petit check de fin
    if resultats:
        print("Fichiers disponibles :")
        for k, v in resultats.items():
            print(f" - {k} : {v}")