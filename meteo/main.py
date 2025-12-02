# main.py
from meteo.pipeline import MeteoPipeline

if __name__ == "__main__":
    pipeline = MeteoPipeline(data_dir="/mnt/data")
    dfs_clean = pipeline.run()

    # Exemple : inspecter un DataFrame nettoyé
    print(dfs_clean["hourly_history"].head())
