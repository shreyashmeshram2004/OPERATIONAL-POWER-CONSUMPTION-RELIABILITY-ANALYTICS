"""Manpur Power Analytics - Gold business-ready datasets."""

from pyspark.sql import functions as F

CATALOG = "manpur_power_analytics"

# ============================================================
# GOLD 1 — VOLTAGE ANALYSIS — CORRECTED NOMINAL REFERENCE
# ============================================================

voltage_gold = (
    spark.table(f"{CATALOG}.silver.voltages_standardized")
    
    .select(
        "bus_name",
        "base_voltage_kv",
        "nominal_voltage_kv",
        "network_level",
        "voltage_average_v",
        "phase_voltage_spread_v"
    )
    
    # Phase-to-neutral nominal voltage
    # = line-to-line nominal voltage / sqrt(3)
    .withColumn(
        "nominal_phase_voltage_v",
        (F.col("nominal_voltage_kv") * 1000) / F.sqrt(F.lit(3))
    )
    
    # Per-unit voltage using the correct phase reference
    .withColumn(
        "average_voltage_pu",
        F.col("voltage_average_v") /
        F.col("nominal_phase_voltage_v")
    )
    
    # Percentage deviation from correct nominal phase voltage
    .withColumn(
        "voltage_deviation_percent",
        (
            (F.col("voltage_average_v") -
             F.col("nominal_phase_voltage_v"))
            / F.col("nominal_phase_voltage_v")
        ) * 100
    )
    
    .withColumn(
        "voltage_status",
        F.when(F.col("average_voltage_pu") < 0.90, "Severe Deviation")
         .when(F.col("average_voltage_pu") < 0.95, "Low Voltage")
         .otherwise("Within Range")
    )
)

# Save corrected Gold table
(
    voltage_gold.write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.voltage_analysis")
)

print("============================================================")
print("✅ GOLD 1 — VOLTAGE ANALYSIS SAVED")
print("============================================================")

print(
    "Rows:",
    spark.table(f"{CATALOG}.gold.voltage_analysis").count()
)

display(
    spark.sql(f"""
        SELECT
            bus_name,
            network_level,
            nominal_voltage_kv,
            nominal_phase_voltage_v,
            voltage_average_v,
            average_voltage_pu,
            voltage_deviation_percent,
            phase_voltage_spread_v,
            voltage_status
        FROM {CATALOG}.gold.voltage_analysis
        ORDER BY
            CASE
                WHEN network_level = '33kV_HT' THEN 1
                WHEN network_level = '11kV_HT' THEN 2
                ELSE 3
            END,
            bus_name
    """)
)


# ============================================================
# GOLD 2 — ASSET LOADING
# ============================================================

asset_loading = (
    spark.table(f"{CATALOG}.silver.seqcurrents_standardized")
    .select(
        "element_name",
        "terminal",
        "current_a",
        "normal_loading_percent",
        "emergency_loading_percent"
    )
    .withColumn(
        "asset_type",
        F.when(F.col("element_name").startswith("Transformer."), "Transformer")
         .when(F.col("element_name").startswith("Line."), "Line")
         .when(F.col("element_name").startswith("Vsource."), "Source")
         .otherwise("Other")
    )
)

(
    asset_loading.write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.asset_loading")
)

print("============================================================")
print("✅ GOLD 2 — ASSET LOADING SAVED")
print("============================================================")

print(
    "Rows:",
    spark.table(f"{CATALOG}.gold.asset_loading").count()
)

display(
    spark.sql(f"""
        SELECT
            element_name,
            asset_type,
            terminal,
            current_a,
            normal_loading_percent,
            emergency_loading_percent
        FROM {CATALOG}.gold.asset_loading
        ORDER BY normal_loading_percent DESC, current_a DESC
    """)
)


# ============================================================
# GOLD 3 — POWER FLOW
# ============================================================

power_flow = (
    spark.table(f"{CATALOG}.silver.seqpowers_standardized")
    .select(
        "element_name",
        "terminal",
        "active_power_kw",
        "reactive_power_kvar",
        "apparent_power_kva",
        "power_factor"
    )
    .withColumn(
        "asset_type",
        F.when(F.col("element_name").startswith("Transformer."), "Transformer")
         .when(F.col("element_name").startswith("Line."), "Line")
         .when(F.col("element_name").startswith("Vsource."), "Source")
         .when(F.col("element_name").startswith("Load."), "Pump Load")
         .otherwise("Other")
    )
)

(
    power_flow.write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.power_flow")
)

print("============================================================")
print("✅ GOLD 3 — POWER FLOW SAVED")
print("============================================================")

print(
    "Rows:",
    spark.table(f"{CATALOG}.gold.power_flow").count()
)

display(
    spark.sql(f"""
        SELECT
            element_name,
            asset_type,
            terminal,
            active_power_kw,
            reactive_power_kvar,
            apparent_power_kva,
            power_factor
        FROM {CATALOG}.gold.power_flow
        ORDER BY apparent_power_kva DESC
    """)
)


# ============================================================
# GOLD 4 — VOLTAGE UNBALANCE
# ============================================================

voltage_unbalance = (
    spark.table(f"{CATALOG}.silver.seqvoltages_standardized")
    .select(
        "bus_name",
        "positive_sequence_voltage_v",
        "voltage_pu",
        "voltage_unbalance_percent",
        "nema_unbalance_percent"
    )
    .withColumn(
        "unbalance_status",
        F.when(
            F.col("voltage_unbalance_percent") >= 1,
            "High Unbalance"
        )
        .when(
            F.col("voltage_unbalance_percent") >= 0.5,
            "Moderate Unbalance"
        )
        .otherwise("Low Unbalance")
    )
)

(
    voltage_unbalance.write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.voltage_unbalance")
)

print("============================================================")
print("✅ GOLD 4 — VOLTAGE UNBALANCE SAVED")
print("============================================================")

print(
    "Rows:",
    spark.table(f"{CATALOG}.gold.voltage_unbalance").count()
)

display(
    spark.sql(f"""
        SELECT *
        FROM {CATALOG}.gold.voltage_unbalance
        ORDER BY voltage_unbalance_percent DESC
    """)
)


# ============================================================
# GOLD 5 — ASSET SUMMARY + GOLD VALIDATION
# ============================================================

# ------------------------------------------------------------
# 1. Aggregate loading information by asset
# ------------------------------------------------------------

loading = (
    spark.table(f"{CATALOG}.gold.asset_loading")
    .groupBy("element_name", "asset_type")
    .agg(
        F.max("current_a").alias("max_current_a"),
        F.max("normal_loading_percent").alias("max_normal_loading_percent"),
        F.max("emergency_loading_percent").alias("max_emergency_loading_percent")
    )
)

# ------------------------------------------------------------
# 2. Aggregate power information by asset
# ------------------------------------------------------------

power = (
    spark.table(f"{CATALOG}.gold.power_flow")
    .groupBy("element_name")
    .agg(
        F.max("apparent_power_kva").alias("max_apparent_power_kva"),
        F.max("active_power_kw").alias("max_active_power_kw"),
        F.max("reactive_power_kvar").alias("max_reactive_power_kvar")
    )
)

# ------------------------------------------------------------
# 3. Combine
# ------------------------------------------------------------

asset_summary = (
    loading
    .join(power, on="element_name", how="left")
    .withColumn(
        "loading_status",
        F.when(
            F.col("max_normal_loading_percent") > 100,
            "Overloaded"
        )
        .when(
            F.col("max_normal_loading_percent") > 80,
            "High Loading"
        )
        .otherwise("Normal / Not Rated")
    )
)

# ------------------------------------------------------------
# 4. Save Gold Asset Summary
# ------------------------------------------------------------

(
    asset_summary.write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.asset_summary")
)

print("============================================================")
print("✅ GOLD 5 — ASSET SUMMARY SAVED")
print("============================================================")

# ------------------------------------------------------------
# 5. Display important assets
# ------------------------------------------------------------

display(
    spark.sql(f"""
        SELECT *
        FROM {CATALOG}.gold.asset_summary
        ORDER BY
            max_normal_loading_percent DESC,
            max_apparent_power_kva DESC
    """)
)

# ------------------------------------------------------------
