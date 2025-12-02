import pandas as pd
from pathlib import Path

# 📌 Find this script's folder
ROOT_DIR = Path(__file__).resolve().parent

# 📁 Input folder for raw CSVs
DATA_DIR = ROOT_DIR / "data"

# 📁 Output folder for cleaned CSVs
OUTPUT_DIR = DATA_DIR / "clean"
OUTPUT_DIR.mkdir(exist_ok=True)


def clean_daily(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["time"] = pd.to_datetime(df["time"], errors="coerce").dt.date
    df = df.dropna(subset=["time"])
    df = df.drop_duplicates(subset=["time"])
    df = df.sort_values("time").reset_index(drop=True)

    num_cols = [
        "temperature_2m_mean",
        "temperature_2m_max",
        "temperature_2m_min",
        "precipitation_sum",
        "windspeed_10m_max",
        "windspeed_10m_mean",
    ]

    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "precipitation_sum" in df.columns:
        df.loc[df["precipitation_sum"] < 0, "precipitation_sum"] = 0

    return df


def clean_hourly(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df = df.dropna(subset=["time"])
    df = df.drop_duplicates(subset=["time"])
    df = df.sort_values("time").reset_index(drop=True)

    num_cols = [
        "temperature_2m",
        "relativehumidity_2m",
        "precipitation",
        "windspeed_10m",
    ]

    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "relativehumidity_2m" in df.columns:
        bad = ~df["relativehumidity_2m"].between(0, 100)
        df.loc[bad, "relativehumidity_2m"] = pd.NA

    if "precipitation" in df.columns:
        df.loc[df["precipitation"] < 0, "precipitation"] = 0

    if "windspeed_10m" in df.columns:
        df.loc[df["windspeed_10m"] < 0, "windspeed_10m"] = 0

    return df


def main():
    # Map filenames → cleaners
    files_daily = {
        "daily_forecast": "meteo_daily_forecast_2025-12-03.csv",
        "daily_history": "meteo_daily_history_2023-01-01_2025-12-02.csv",
    }

    files_hourly = {
        "hourly_forecast": "meteo_hourly_forecast_2025-12-03.csv",
        "hourly_history": "meteo_hourly_history_2023-01-01_2025-12-02.csv",
    }

    # --- Process DAILY files ---
    for out_name, filename in files_daily.items():
        path = DATA_DIR / filename
        if not path.exists():
            print(f"⚠️ File not found: {path}")
            continue

        df = pd.read_csv(path)
        df_clean = clean_daily(df)
        df_clean.to_csv(OUTPUT_DIR / f"{out_name}_clean.csv", index=False)
        print(f"✅ Saved: {OUTPUT_DIR}/{out_name}_clean.csv")

    # --- Process HOURLY files ---
    for out_name, filename in files_hourly.items():
        path = DATA_DIR / filename
        if not path.exists():
            print(f"⚠️ File not found: {path}")
            continue

        df = pd.read_csv(path)
        df_clean = clean_hourly(df)
        df_clean.to_csv(OUTPUT_DIR / f"{out_name}_clean.csv", index=False)
        print(f"✅ Saved: {OUTPUT_DIR}/{out_name}_clean.csv")

    print("\n🎉 All cleaned CSVs saved in:", OUTPUT_DIR)


if __name__ == "__main__":
    main()
