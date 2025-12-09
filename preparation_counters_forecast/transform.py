import pandas as pd

def build_forecast_dataset(df_counters, df_meteo, calendar_info, target_date):
    """Assemble les données pour créer les 240 lignes de prédiction."""
    print("   [Transform] Assemblage des données...")
    
    final_rows = []
    
    # Parsing des infos calendrier
    is_ferie = int(calendar_info.get('is_ferie', 0))
    is_vacances = int(calendar_info.get('is_vacances', 0))

    # Double boucle : Compteurs x 24 Heures
    for _, counter in df_counters.iterrows():
        for hour in range(24):
            # Trouver la météo de l'heure h
            meteo_h = df_meteo[df_meteo['hour_key'] == hour]
            if not meteo_h.empty:
                meteo_h = meteo_h.iloc[0]
            else:
                meteo_h = df_meteo.iloc[0] # Fallback
            
            # Gestion Date/Heure
            ts = pd.Timestamp(f"{target_date} {hour:02d}:00:00")
            
            # Calculs Temporels
            is_weekend = 1 if ts.weekday() >= 5 else 0
            is_jour_ouvre = 1 if (is_weekend == 0 and is_ferie == 0) else 0
            
            # Calculs Météo
            precip = float(meteo_h.get('precipitation', 0))
            is_raining = 1 if precip > 0 else 0
            
            precip_class = 0
            if precip > 0 and precip < 0.5: precip_class = 1
            elif precip >= 0.5 and precip < 4: precip_class = 2
            elif precip >= 4: precip_class = 3

            # Construction de la ligne
            row = {
                "name": counter['name'],
                "timestamp": ts.isoformat(),
                "intensity": 0, # Placeholder pour la prédiction future
                "latitude": counter['latitude'],
                "longitude": counter['longitude'],
                
                # Météo
                "temperature_2m": float(meteo_h.get('temperature_2m', 0)),
                "precipitation": precip,
                "windspeed_10m": float(meteo_h.get('windspeed_10m', 0)),
                "precipitation_class": precip_class,
                "is_raining": is_raining,
                
                # Calendrier
                "jour_semaine": ts.weekday(),
                "is_weekend": is_weekend,
                "nom_jour": ts.strftime("%A"),
                "is_ferie": is_ferie,
                "is_vacances": is_vacances,
                "is_jour_ouvre": is_jour_ouvre
            }
            final_rows.append(row)
            
    return pd.DataFrame(final_rows)