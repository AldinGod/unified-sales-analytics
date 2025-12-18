import glob
import os

import pandas as pd
from sqlalchemy import create_engine


def main():
    # 1) Konfiguracija za Postgres
    pg_host = os.environ.get("PG_HOST", "localhost")
    pg_port = os.environ.get("PG_PORT", "5432")
    pg_db = os.environ.get("PG_DB", "unified_sales_analytics")
    pg_user = os.environ.get("PG_USER", os.environ.get("USER", "aldingodinjak"))
    pg_password = os.environ.get("PG_PASSWORD", "")

    if pg_password:
        conn_str = f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    else:
        conn_str = f"postgresql+psycopg2://{pg_user}@{pg_host}:{pg_port}/{pg_db}"

    print(f"🔗 Connecting to Postgres: {conn_str}")
    engine = create_engine(conn_str)

    # 2) Nađi sve Parquet fajlove u Gold folderu
    gold_path = "delta-lake/gold/sales_daily_metrics"
    pattern = os.path.join(gold_path, "*.parquet")
    files = glob.glob(pattern)

    if not files:
        print(f"⚠️ No Parquet files found in {gold_path}")
        return

    print(f"📁 Found {len(files)} Parquet file(s) in {gold_path}")

    # 3) Učitaj sve Parquet fajlove u jedan pandas DataFrame
    dfs = []
    for f in files:
        print(f"📥 Reading {f}")
        dfs.append(pd.read_parquet(f))

    df = pd.concat(dfs, ignore_index=True)
    print(f"✅ Combined dataframe shape: {df.shape}")
    print(df.head())

    # 4) Upis u Postgres tabelu "sales_daily_metrics"
    target_table = "sales_daily_metrics"
    print(f"📤 Writing dataframe to Postgres table '{target_table}' (replace)...")

    df.to_sql(target_table, engine, if_exists="replace", index=False)

    print("🎉 Finished writing Gold metrics to Postgres via pandas/SQLAlchemy!")


if __name__ == "__main__":
    main()
