"""Manpur Power Analytics - Bronze ingestion."""

import pandas as pd

BASE_PATH = "/Volumes/manpur_power_analytics/bronze/raw"

FILES = {
    "voltages": "CaseStudy_EXP_VOLTAGES 1.csv",
    "seqcurrents": "CaseStudy_EXP_SEQCURRENTS 1.csv",
    "seqpowers": "CaseStudy_EXP_SEQPOWERS 1.csv",
    "seqvoltages": "CaseStudy_EXP_SEQVOLTAGES 1.csv",
}


def clean_column_names(df):
    df = df.copy()
    df.columns = (
        df.columns
        .str.strip()
        .str.replace(" ", "_", regex=False)
        .str.replace("%", "Percent", regex=False)
        .str.replace("/", "_", regex=False)
        .str.replace("(", "", regex=False)
        .str.replace(")", "", regex=False)
    )
    return df


datasets = {
    name: pd.read_csv(f"{BASE_PATH}/{filename}", encoding="latin1")
    for name, filename in FILES.items()
}

for name, df in datasets.items():
    df = clean_column_names(df)
    df = df.dropna(how="all").reset_index(drop=True)
    df["source_file"] = FILES[name]
    df["source_row_id"] = range(1, len(df) + 1)
    datasets[name] = df


df_voltages = datasets["voltages"]
df_seqcurrents = datasets["seqcurrents"]
df_seqpowers = datasets["seqpowers"]
df_seqvoltages = datasets["seqvoltages"]

print("Bronze ingestion complete.")
print({name: df.shape for name, df in datasets.items()})
