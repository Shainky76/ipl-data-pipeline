import pandas as pd
import sqlite3

conn = sqlite3.connect("/Users/shainky/Downloads/archive/ipl.db")
df = pd.read_csv("/Users/shainky/Downloads/archive/matches.csv")
df.to_sql("matches", conn, if_exists="replace", index=False)

print(f"loaded {len(df)} rows into matches table")
conn.close()
