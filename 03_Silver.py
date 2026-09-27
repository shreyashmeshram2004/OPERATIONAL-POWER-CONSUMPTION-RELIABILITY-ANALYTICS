"""Manpur Power Analytics - Silver cleaning and standardization.

Cleans Bronze data, stores Silver Delta tables, then creates standardized
Silver tables used by the Gold layer.
"""

import pandas as pd
import numpy as np

BASE_PATH = "/Volumes/manpur_power_analytics/bronze/raw"
CATALOG = "manpur_power_analytics"

# Reload the Bronze CSVs so this script is independently runnable.
df_voltages = pd.read_csv(f"{BASE_PATH}/CaseStudy_EXP_VOLTAGES 1.csv", encoding="latin1")
df_seqcurrents = pd.read_csv(f"{BASE_PATH}/CaseStudy_EXP_SEQCURRENTS 1.csv", encoding="latin1")
df_seqpowers = pd.read_csv(f"{BASE_PATH}/CaseStudy_EXP_SEQPOWERS 1.csv", encoding="latin1")
df_seqvoltages = pd.read_csv(f"{BASE_PATH}/CaseStudy_EXP_SEQVOLTAGES 1.csv", encoding="latin1")

def clean_column_names(df):
    df = df.copy()
    df.columns = (df.columns.str.strip().str.replace(" ", "_", regex=False)
                  .str.replace("%", "Percent", regex=False)
                  .str.replace("/", "_", regex=False)
                  .str.replace("(", "", regex=False).str.replace(")", "", regex=False))
    return df

df_voltages = clean_column_names(df_voltages).dropna(how="all").reset_index(drop=True)
df_seqcurrents = clean_column_names(df_seqcurrents).dropna(how="all").reset_index(drop=True)
df_seqpowers = clean_column_names(df_seqpowers).dropna(how="all").reset_index(drop=True)
df_seqvoltages = clean_column_names(df_seqvoltages).dropna(how="all").reset_index(drop=True)

source_files = {"voltages":"CaseStudy_EXP_VOLTAGES 1.csv","seqcurrents":"CaseStudy_EXP_SEQCURRENTS 1.csv","seqpowers":"CaseStudy_EXP_SEQPOWERS 1.csv","seqvoltages":"CaseStudy_EXP_SEQVOLTAGES 1.csv"}
for name, df in {"voltages":df_voltages,"seqcurrents":df_seqcurrents,"seqpowers":df_seqpowers,"seqvoltages":df_seqvoltages}.items():
    df["source_file"] = source_files[name]
    df["source_row_id"] = range(1, len(df)+1)

# Base Silver cleaning
silver_voltages = df_voltages.copy()
silver_seqcurrents = df_seqcurrents.copy()
silver_seqpowers = df_seqpowers.copy()
silver_seqvoltages = df_seqvoltages.copy()

for df in [silver_voltages, silver_seqcurrents, silver_seqpowers, silver_seqvoltages]:
    df.dropna(how="all", inplace=True)

silver_voltages["Bus"] = silver_voltages["Bus"].astype(str).str.strip()
silver_seqcurrents["Element"] = silver_seqcurrents["Element"].astype(str).str.strip()
silver_seqpowers["Element"] = silver_seqpowers["Element"].astype(str).str.strip()
silver_seqvoltages["Bus"] = silver_seqvoltages["Bus"].astype(str).str.strip()

voltage_numeric = ["BasekV","Node1","Magnitude1","Angle1","pu1","Node2","Magnitude2","Angle2","pu2","Node3","Magnitude3","Angle3","pu3","Node4","Magnitude4","Angle4","pu4","source_row_id"]
current_numeric = ["Terminal","I1","PercentNormal","PercentEmergency","I2","PercentI2_I1","I0","PercentI0_I1","Iresidual","PercentNEMA","source_row_id"]
power_numeric = ["Terminal","P1kW","Q1kvar","P2","Q2","P0","Q0","P_Normal","Q_Normal","P_Emergency","Q_Emergency","source_row_id"]
seqvoltage_numeric = ["V1","p.u.","Base_kV","V2","PercentV2_V1","V0","PercentV0_V1","Vresidual","PercentNEMA","source_row_id"]

for col in voltage_numeric:
    if col in silver_voltages.columns: silver_voltages[col] = pd.to_numeric(silver_voltages[col], errors="coerce")
for col in current_numeric:
    if col in silver_seqcurrents.columns: silver_seqcurrents[col] = pd.to_numeric(silver_seqcurrents[col], errors="coerce")
for col in power_numeric:
    if col in silver_seqpowers.columns: silver_seqpowers[col] = pd.to_numeric(silver_seqpowers[col], errors="coerce")
for col in seqvoltage_numeric:
    if col in silver_seqvoltages.columns: silver_seqvoltages[col] = pd.to_numeric(silver_seqvoltages[col], errors="coerce")

silver_voltages = silver_voltages.drop_duplicates().reset_index(drop=True)
silver_seqcurrents = silver_seqcurrents.drop_duplicates().reset_index(drop=True)
silver_seqpowers = silver_seqpowers.drop_duplicates().reset_index(drop=True)
silver_seqvoltages = silver_seqvoltages.drop_duplicates().reset_index(drop=True)

silver_voltages = silver_voltages[silver_voltages["Bus"].notna() & (silver_voltages["Bus"] != "")].copy()
silver_seqcurrents = silver_seqcurrents[silver_seqcurrents["Element"].notna() & (silver_seqcurrents["Element"] != "")].copy()
silver_seqpowers = silver_seqpowers[silver_seqpowers["Element"].notna() & (silver_seqpowers["Element"] != "")].copy()
silver_seqvoltages = silver_seqvoltages[silver_seqvoltages["Bus"].notna() & (silver_seqvoltages["Bus"] != "")].copy()

silver_seqvoltages.drop(columns=["Unnamed:_2"], errors="ignore", inplace=True)
silver_seqpowers["has_normal_limit"] = silver_seqpowers["P_Normal"].notna() | silver_seqpowers["Q_Normal"].notna()
silver_seqpowers["has_emergency_limit"] = silver_seqpowers["P_Emergency"].notna() | silver_seqpowers["Q_Emergency"].notna()

for name, df in {"voltages":silver_voltages,"seqcurrents":silver_seqcurrents,"seqpowers":silver_seqpowers,"seqvoltages":silver_seqvoltages}.items():
    spark.createDataFrame(df).write.mode("overwrite").format("delta").saveAsTable(f"{CATALOG}.silver.{name}")

# ============================================================

import pandas as pd
import numpy as np

CATALOG = "manpur_power_analytics"


# ------------------------------------------------------------
# 1. LOAD THE ORIGINAL SAVED SILVER TABLES
# ------------------------------------------------------------

v = spark.table(f"{CATALOG}.silver.voltages").toPandas()
c = spark.table(f"{CATALOG}.silver.seqcurrents").toPandas()
p = spark.table(f"{CATALOG}.silver.seqpowers").toPandas()
sv = spark.table(f"{CATALOG}.silver.seqvoltages").toPandas()


# ------------------------------------------------------------
# 3. HELPER: FIND COLUMN WITHOUT ASSUMING EXACT SPELLING
# ------------------------------------------------------------

def find_col(df, candidates):
    lookup = {
        str(col).strip().lower().replace(" ", "").replace("_", ""): col
        for col in df.columns
    }

    for candidate in candidates:
        key = (
            candidate
            .strip()
            .lower()
            .replace(" ", "")
            .replace("_", "")
        )

        if key in lookup:
            return lookup[key]

    raise KeyError(
        f"Could not find any of {candidates}\n"
        f"Available columns: {df.columns.tolist()}"
    )


# ============================================================
# 4. VOLTAGES
# ============================================================

bus_col = find_col(v, ["Bus"])
base_kv_col = find_col(v, ["BasekV", "Base_kV", "Base kV"])

mag1_col = find_col(v, ["Magnitude1"])
mag2_col = find_col(v, ["Magnitude2"])
mag3_col = find_col(v, ["Magnitude3"])

final_voltages = pd.DataFrame()

final_voltages["bus_name"] = v[bus_col].astype(str).str.strip()

final_voltages["base_voltage_kv"] = pd.to_numeric(
    v[base_kv_col],
    errors="coerce"
)

final_voltages["voltage_phase_a_v"] = pd.to_numeric(
    v[mag1_col],
    errors="coerce"
)

final_voltages["voltage_phase_b_v"] = pd.to_numeric(
    v[mag2_col],
    errors="coerce"
)

final_voltages["voltage_phase_c_v"] = pd.to_numeric(
    v[mag3_col],
    errors="coerce"
)


# Normalize 0.42 kV / 0.415 kV representation
final_voltages["nominal_voltage_kv"] = np.where(
    final_voltages["base_voltage_kv"] < 1,
    0.415,
    final_voltages["base_voltage_kv"]
)

final_voltages["nominal_voltage_v"] = (
    final_voltages["nominal_voltage_kv"] * 1000
)


# Network classification
final_voltages["network_level"] = np.select(
    [
        final_voltages["nominal_voltage_kv"] >= 30,
        final_voltages["nominal_voltage_kv"].between(1, 20),
        final_voltages["nominal_voltage_kv"] < 1
    ],
    [
        "33kV_HT",
        "11kV_HT",
        "415V_LT"
    ],
    default="Unknown"
)


# Voltage statistics
phase_cols = [
    "voltage_phase_a_v",
    "voltage_phase_b_v",
    "voltage_phase_c_v"
]

final_voltages["voltage_average_v"] = (
    final_voltages[phase_cols].mean(axis=1)
)

final_voltages["voltage_min_v"] = (
    final_voltages[phase_cols].min(axis=1)
)

final_voltages["voltage_max_v"] = (
    final_voltages[phase_cols].max(axis=1)
)

final_voltages["voltage_average_kv"] = (
    final_voltages["voltage_average_v"] / 1000
)

final_voltages["voltage_deviation_v"] = (
    final_voltages["voltage_average_v"]
    - final_voltages["nominal_voltage_v"]
)

final_voltages["voltage_deviation_percent"] = (
    final_voltages["voltage_deviation_v"]
    / final_voltages["nominal_voltage_v"]
) * 100

final_voltages["phase_voltage_spread_v"] = (
    final_voltages["voltage_max_v"]
    - final_voltages["voltage_min_v"]
)

final_voltages["phase_voltage_spread_percent"] = (
    final_voltages["phase_voltage_spread_v"]
    / final_voltages["voltage_average_v"]
) * 100


# Keep source lineage
if "source_file" in v.columns:
    final_voltages["source_file"] = v["source_file"]

if "source_row_id" in v.columns:
    final_voltages["source_row_id"] = v["source_row_id"]


# ============================================================
# 5. CURRENTS
# ============================================================

element_col = find_col(c, ["Element"])
terminal_col = find_col(c, ["Terminal"])
i1_col = find_col(c, ["I1"])
i2_col = find_col(c, ["I2"])
i0_col = find_col(c, ["I0"])
residual_col = find_col(c, ["Iresidual"])
i2_percent_col = find_col(c, ["%I2/I1", "PercentI2_I1"])
normal_col = find_col(c, ["%Normal", "PercentNormal"])
emergency_col = find_col(c, ["%Emergency", "PercentEmergency"])
nema_col = find_col(c, ["%NEMA", "PercentNEMA"])

final_currents = pd.DataFrame()

final_currents["element_name"] = (
    c[element_col].astype(str).str.strip()
)

final_currents["terminal"] = c[terminal_col]

final_currents["current_a"] = pd.to_numeric(
    c[i1_col], errors="coerce"
)

final_currents["negative_sequence_current_a"] = pd.to_numeric(
    c[i2_col], errors="coerce"
)

final_currents["zero_sequence_current_a"] = pd.to_numeric(
    c[i0_col], errors="coerce"
)

final_currents["residual_current_a"] = pd.to_numeric(
    c[residual_col], errors="coerce"
)

final_currents["current_unbalance_percent"] = pd.to_numeric(
    c[i2_percent_col], errors="coerce"
)

final_currents["normal_loading_percent"] = pd.to_numeric(
    c[normal_col], errors="coerce"
)

final_currents["emergency_loading_percent"] = pd.to_numeric(
    c[emergency_col], errors="coerce"
)

final_currents["nema_unbalance_percent"] = pd.to_numeric(
    c[nema_col], errors="coerce"
)

if "source_file" in c.columns:
    final_currents["source_file"] = c["source_file"]

if "source_row_id" in c.columns:
    final_currents["source_row_id"] = c["source_row_id"]


# ============================================================
# 6. POWERS
# ============================================================

element_col = find_col(p, ["Element"])
terminal_col = find_col(p, ["Terminal"])
p1_col = find_col(p, ["P1kW", "P1(kW)"])
q1_col = find_col(p, ["Q1kvar", "Q1(kvar)"])

final_powers = pd.DataFrame()

final_powers["element_name"] = (
    p[element_col].astype(str).str.strip()
)

final_powers["terminal"] = p[terminal_col]

final_powers["active_power_kw"] = pd.to_numeric(
    p[p1_col], errors="coerce"
)

final_powers["reactive_power_kvar"] = pd.to_numeric(
    p[q1_col], errors="coerce"
)

final_powers["apparent_power_kva"] = np.sqrt(
    final_powers["active_power_kw"] ** 2
    + final_powers["reactive_power_kvar"] ** 2
)

final_powers["power_factor"] = np.where(
    final_powers["apparent_power_kva"] > 0,
    final_powers["active_power_kw"]
    / final_powers["apparent_power_kva"],
    np.nan
)

if "source_file" in p.columns:
    final_powers["source_file"] = p["source_file"]

if "source_row_id" in p.columns:
    final_powers["source_row_id"] = p["source_row_id"]


# ============================================================
# 7. SEQUENCE VOLTAGES
# ============================================================

bus_col = find_col(sv, ["Bus"])
v1_col = find_col(sv, ["V1"])
v2_col = find_col(sv, ["V2"])
v0_col = find_col(sv, ["V0"])
v_residual_col = find_col(sv, ["Vresidual"])
v2_percent_col = find_col(sv, ["%V2/V1", "PercentV2_V1"])
v0_percent_col = find_col(sv, ["%V0/V1", "PercentV0_V1"])
nema_col = find_col(sv, ["%NEMA", "PercentNEMA"])
pu_col = find_col(sv, ["p.u.", "pu"])

final_seqvoltages = pd.DataFrame()

final_seqvoltages["bus_name"] = (
    sv[bus_col].astype(str).str.strip()
)

final_seqvoltages["positive_sequence_voltage_v"] = pd.to_numeric(
    sv[v1_col], errors="coerce"
)

final_seqvoltages["negative_sequence_voltage_v"] = pd.to_numeric(
    sv[v2_col], errors="coerce"
)

final_seqvoltages["zero_sequence_voltage_v"] = pd.to_numeric(
    sv[v0_col], errors="coerce"
)

final_seqvoltages["residual_voltage_v"] = pd.to_numeric(
    sv[v_residual_col], errors="coerce"
)

final_seqvoltages["voltage_pu"] = pd.to_numeric(
    sv[pu_col], errors="coerce"
)

final_seqvoltages["voltage_unbalance_percent"] = pd.to_numeric(
    sv[v2_percent_col], errors="coerce"
)

final_seqvoltages["zero_sequence_percent"] = pd.to_numeric(
    sv[v0_percent_col], errors="coerce"
)

final_seqvoltages["nema_unbalance_percent"] = pd.to_numeric(
    sv[nema_col], errors="coerce"
)

if "source_file" in sv.columns:
    final_seqvoltages["source_file"] = sv["source_file"]

if "source_row_id" in sv.columns:
    final_seqvoltages["source_row_id"] = sv["source_row_id"]


# ============================================================

# Save standardized Silver tables
spark.createDataFrame(final_voltages).write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.silver.voltages_standardized")
spark.createDataFrame(final_currents).write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.silver.seqcurrents_standardized")
spark.createDataFrame(final_powers).write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.silver.seqpowers_standardized")
spark.createDataFrame(final_seqvoltages).write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.silver.seqvoltages_standardized")
