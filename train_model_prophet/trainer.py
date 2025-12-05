# train_model/trainer.py
from prophet import Prophet

def train_model(train_df):
    """Configure et entraîne le modèle Prophet."""
    
    # Configuration
    m = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=True,
        seasonality_mode='multiplicative'
    )

    # Ajout des régresseurs (Facteurs externes)
    regressors = [
        'temperature_2m', 'precipitation', 'windspeed_10m',
        'is_vacances', 'is_ferie', 'is_weekend'
    ]
    # ajout de 'precipitation_class' dégrade le résultat, idem si lancé seul ou lancé avec precipitation
    # par contre precipitation seul donne le meilleur sur les 3 tests
    for reg in regressors:
        m.add_regressor(reg)

    # Entraînement
    m.fit(train_df)
    return m

def make_predictions(model, test_df):
    """Génère les prédictions sur le jeu de test."""
    forecast = model.predict(test_df)
    return forecast