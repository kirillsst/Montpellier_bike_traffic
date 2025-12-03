# data_calendrier/main.py
from pipeline import CalendarPipeline

if __name__ == "__main__":
    # On lance le pipeline
    # Les données seront enregistrées dans data_calendrier/data/
    pipe = CalendarPipeline(output_dir="data")
    pipe.run()