import pandas as pd
from geopy.distance import geodesic

def match_counters(df_old, df_new, max_distance_m=10):
    """
    Comparaison des anciens et nouveaux capteurs par coordonnées.
    max_distance_m — distance maximale autorisée (en mètres)
    """
    matches = []

    for _, old in df_old.iterrows():
        old_coord = tuple(old['location']['value']['coordinates'])  # (lat, lon)
        for _, new in df_new.iterrows():
            new_coord = tuple(new['location']['value']['coordinates'])
            distance = geodesic(old_coord, new_coord).meters
            if distance <= max_distance_m:
                matches.append({
                    "old_id": old["id"],
                    "new_id": new["id"],
                    "distance_m": distance
                })
    return pd.DataFrame(matches)
