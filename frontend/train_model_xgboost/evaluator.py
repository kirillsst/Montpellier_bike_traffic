# train_model_xgboost/evaluator.py
from sklearn.metrics import mean_absolute_error

def evaluate(y_true, y_pred):
    mae = mean_absolute_error(y_true, y_pred)
    
    mean_val = y_true.mean()
    error_pct = (mae / mean_val) * 100 if mean_val > 0 else 0
    
    return round(mae, 2), round(error_pct, 2)