from sqlalchemy import create_engine, text
import pandas as pd

local_url = "postgresql://postgres:2491@localhost:5432/tasarruf_finansman"
neon_url = "postgresql://neondb_owner:npg_X9aygcQ3EHlZ@ep-super-term-al9k62e1.c-3.eu-central-1.aws.neon.tech/neondb?sslmode=require"

local_engine = create_engine(local_url)
neon_engine = create_engine(neon_url)

tablolar = ["dim_date", "dim_plan", "dim_member", "fact_payments"]

for tablo in tablolar:
    print(f"Taşınıyor: {tablo}...", end=" ")
    df = pd.read_sql(f"SELECT * FROM {tablo}", local_engine)
    df.to_sql(tablo, neon_engine, if_exists="replace", index=False)
    print(f"✅ {len(df):,} satır")

print("\n🎉 Tüm tablolar Neon'a taşındı!")