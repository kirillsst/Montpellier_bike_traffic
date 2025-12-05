# train_model/evaluator.py
from sklearn.metrics import mean_absolute_error

def evaluate(test_df, forecast_df):
    """Calcule les métriques de performance."""
    y_true = test_df['y']
    y_pred = forecast_df['yhat']
    
    mae = mean_absolute_error(y_true, y_pred)
    
    # Erreur relative (%)
    mean_val = y_true.mean()
    error_pct = (mae / mean_val) * 100 if mean_val > 0 else 0
    
    return round(mae, 2), round(error_pct, 2)