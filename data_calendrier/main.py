# data_calendrier/main.py
from data_calendrier.pipeline import CalendarPipeline

def run_pipeline():
    """
    Exécute le pipeline calendrier et retourne éventuellement un résumé.
    """
    pipe = CalendarPipeline(output_dir="data")
    result = pipe.run()
    return {"status": "ok", "message": "Pipeline calendrier exécuté", "result": result}

if __name__ == "__main__":
    run_pipeline()
