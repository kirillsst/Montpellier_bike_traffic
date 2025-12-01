import requests
import pandas as pd
from src.api.config import BASE_URL

# BASE = "https://portail-api-data.montpellier3m.fr"

# def fetch_all_counters():
#     url = f"{BASE}/ecocounter"
#     limit = 1000
#     offset = 0

#     all_results = []

#     while True:
#         full_url = f"{url}?limit={limit}&offset={offset}"
#         response = requests.get(full_url).json()

#         if not isinstance(response, list):
#             raise ValueError(f"Unexpected API response: {response}")

#         if len(response) == 0:
#             break

#         all_results.extend(response)
#         offset += limit

#     return pd.DataFrame(all_results)

def fetch_api_counters_list():
    url = f"{BASE_URL}/ecocounter"
    limit, offset = 1000, 0
    all_results = []

    while True:
        r = requests.get(f"{url}?limit={limit}&offset={offset}")
        r.raise_for_status()
        data = r.json()
        if not data:
            break
        all_results.extend(data)
        if len(data) < limit:
            break
        offset += limit

    df = pd.DataFrame(all_results)
    if df.empty or "id" not in df.columns:
        return pd.DataFrame()
    
    df["serial_number"] = df["id"].apply(lambda x: str(x).split(':')[-1].strip() if x else "")
    return df

def fetch_counter_timeseries(counter_id, start_date, end_date):
    url = f"{BASE_URL}/ecocounter_timeseries/{counter_id}/attrs/intensity"
    params = {"fromDate": start_date, "toDate": end_date}
    
    r = requests.get(url, params=params)
    r.raise_for_status()
    ts = r.json()
    
    if isinstance(ts, dict) and "index" in ts and "values" in ts and len(ts["index"]) > 0:
        df_temp = pd.DataFrame({
            "timestamp": pd.to_datetime(ts["index"]),
            "intensity": ts["values"]
        })
        return df_temp
    return pd.DataFrame()
