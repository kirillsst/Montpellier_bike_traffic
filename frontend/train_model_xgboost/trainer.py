# train_model_xgboost/trainer.py
import xgboost as xgb

def train_model(X_train, y_train):
    """Entraîne un régresseur XGBoost."""
    
    # Configuration "Standard Robuste" pour séries temporelles
    model = xgb.XGBRegressor(
        n_estimators=1000,      # Nombre d'arbres
        learning_rate=0.05,     # Vitesse d'apprentissage (plus petit = plus précis mais lent)
        max_depth=5,            # Complexité de l'arbre
        early_stopping_rounds=50, # Arrête si ça ne s'améliore plus
        objective='reg:squarederror',
        n_jobs=-1               # Utilise tous les coeurs CPU
    )

    # On utilise une partie du train comme validation interne pour le early_stopping
    # (XGBoost a besoin de savoir quand s'arrêter)
    eval_set = [(X_train, y_train)]
    
    model.fit(
        X_train, y_train,
        eval_set=eval_set,
        verbose=False
    )
    
    return model

def make_predictions(model, X_test):
    """Prédiction simple."""
    return model.predict(X_test)