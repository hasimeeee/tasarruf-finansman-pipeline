from sqlalchemy import create_engine, text
import pandas as pd
import os

local_url = os.getenv("LOCAL_DB_URL")
neon_url = os.getenv("NEON_DB_URL")

local_engine = create_engine(local_url)
neon_engine = create_engine(neon_url)

tablolar = ["dim_date", "dim_plan", "dim_member", "fact_payments"]

for tablo in tablolar:
    print(f"Taşınıyor: {tablo}...", end=" ")
    df = pd.read_sql(f"SELECT * FROM {tablo}", local_engine)
    df.to_sql(tablo, neon_engine, if_exists="replace", index=False)
    print(f"✅ {len(df):,} satır")

print("\n🎉 Tüm tablolar Neon'a taşındı!")
