import pandas as pd
import os

def read_file(filename):
    if not os.path.exists(filename):
        print(f"file not found: {filename}")
        return None
    df=pd.read_csv(filename)
    print(f"file loaded : {len(df)} rows")
    return df

def filter_data(df):
    df_filter=df[df["dl_applied"]== 1]
    print(f"filtered: {len(df_filter)} rows")
    return df_filter

def save_data(df,output_path):
    df.to_csv(output_path, index=False)
    print(f"saved to {output_path}")

df=read_file("/Users/shainky/Downloads/archive/matches.csv")
if df is not None:
    filter_file=filter_data(df)
    save_data(filter_file, "/Users/shainky/Downloads/archive/matches_filter.csv")

