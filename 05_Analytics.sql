%md
Analytics 1 — Voltage Performance
Business question
Where are the worst voltage conditions in the Manpur distribution network?

%sql
-- ============================================================
-- ANALYTICS 1 — VOLTAGE PERFORMANCE
-- Business Question:
-- Where are the worst voltage conditions?
-- ============================================================
CREATE OR REPLACE TABLE manpur_power_analytics.analytics.voltage_performance
USING DELTA
AS
SELECT
    bus_name,
    network_level,
    nominal_voltage_kv,
    nominal_phase_voltage_v,
    voltage_average_v,
    average_voltage_pu,
    voltage_deviation_percent,
    phase_voltage_spread_v,
    voltage_status,
    ROW_NUMBER() OVER (
        ORDER BY voltage_deviation_percent ASC
    ) AS voltage_risk_rank
FROM manpur_power_analytics.gold.voltage_analysis;
-- ============================================================
CREATE OR REPLACE TABLE manpur_power_analytics.analytics.voltage_deterioration
USING DELTA
AS
WITH lt_data AS (
    SELECT
        bus_name,
        nominal_voltage_kv,
        voltage_average_v,
        average_voltage_pu,
        voltage_deviation_percent,
        phase_voltage_spread_v,
        voltage_status
    FROM manpur_power_analytics.analytics.voltage_performance
    WHERE network_level = '415V_LT'
),
lt_reference AS (
    SELECT
        MAX(
            CASE
                WHEN bus_name = 'LTBUS1'
                THEN voltage_average_v
            END
        ) AS dt_side_reference_voltage_v
    FROM lt_data
)
SELECT
    l.bus_name,
    l.nominal_voltage_kv,
    l.voltage_average_v,
    l.average_voltage_pu,
    l.voltage_deviation_percent,
    l.phase_voltage_spread_v,
    l.voltage_status,
    r.dt_side_reference_voltage_v,
    l.voltage_average_v
        - r.dt_side_reference_voltage_v
        AS voltage_change_from_dt_side_v,
    (
        (l.voltage_average_v
        - r.dt_side_reference_voltage_v)
        / r.dt_side_reference_voltage_v
    ) * 100
        AS voltage_change_from_dt_side_percent
FROM lt_data l
CROSS JOIN lt_reference r;
-- ============================================================
CREATE OR REPLACE TABLE manpur_power_analytics.analytics.asset_demand
USING DELTA
AS
WITH asset_data AS (
    SELECT
        element_name,
        asset_type,
        terminal,
        active_power_kw,
        reactive_power_kvar,
        apparent_power_kva,
        power_factor
    FROM manpur_power_analytics.gold.power_flow
    WHERE terminal = 1
      AND asset_type IN ('Transformer', 'Line')
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            ORDER BY apparent_power_kva DESC
        ) AS demand_rank
    FROM asset_data
)
SELECT
    demand_rank,
    element_name,
    asset_type,
    active_power_kw,
    reactive_power_kvar,
    apparent_power_kva,
    power_factor
FROM ranked;
-- ============================================================
-- 3B. PUMP DEMAND RANKING
-- ============================================================
CREATE OR REPLACE TABLE manpur_power_analytics.analytics.pump_demand
USING DELTA
AS
WITH pump_data AS (
    SELECT
        element_name,
        active_power_kw,
        reactive_power_kvar,
        apparent_power_kva,
        power_factor
    FROM manpur_power_analytics.gold.power_flow
    WHERE asset_type = 'Pump Load'
      AND terminal = 1
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            ORDER BY apparent_power_kva DESC
        ) AS demand_rank
    FROM pump_data
)
SELECT
    demand_rank,
    element_name AS pump_name,
    active_power_kw,
    reactive_power_kvar,
    apparent_power_kva,
    power_factor
FROM ranked;
-- ============================================================
CREATE OR REPLACE TABLE manpur_power_analytics.analytics.pump_demand_contribution
USING DELTA
AS
WITH pump_data AS (
    SELECT
        pump_name,
        active_power_kw,
        reactive_power_kvar,
        apparent_power_kva,
        power_factor
    FROM manpur_power_analytics.analytics.pump_demand
),
totals AS (
    SELECT
        SUM(active_power_kw) AS total_active_power_kw,
        SUM(reactive_power_kvar) AS total_reactive_power_kvar,
        SUM(apparent_power_kva) AS total_apparent_power_kva
    FROM pump_data
),
ranked AS (
    SELECT
        p.*,
        t.total_active_power_kw,
        t.total_reactive_power_kvar,
        t.total_apparent_power_kva,
        (
            p.active_power_kw
            / t.total_active_power_kw
        ) * 100 AS active_power_contribution_percent,
        (
            p.apparent_power_kva
            / t.total_apparent_power_kva
        ) * 100 AS apparent_power_contribution_percent,
        SUM(p.apparent_power_kva) OVER (
            ORDER BY p.apparent_power_kva DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        / t.total_apparent_power_kva * 100
        AS cumulative_apparent_power_percent
    FROM pump_data p
    CROSS JOIN totals t
)
SELECT
    ROW_NUMBER() OVER (
        ORDER BY apparent_power_kva DESC
    ) AS demand_rank,
    pump_name,
    active_power_kw,
    reactive_power_kvar,
    apparent_power_kva,
    power_factor,
    total_active_power_kw,
    total_reactive_power_kvar,
    total_apparent_power_kva,
    active_power_contribution_percent,
    apparent_power_contribution_percent,
    cumulative_apparent_power_percent
FROM ranked;
-- ============================================================
-- 4B. PUMP DEMAND KPI SUMMARY
-- ============================================================
CREATE OR REPLACE TABLE manpur_power_analytics.analytics.pump_demand_kpi
USING DELTA
AS
SELECT
    COUNT(*) AS pump_count,
    SUM(active_power_kw)
        AS total_active_power_kw,
    SUM(reactive_power_kvar)
        AS total_reactive_power_kvar,
    SUM(apparent_power_kva)
        AS total_apparent_power_kva,
    AVG(power_factor)
        AS average_power_factor,
    MAX(apparent_power_kva)
        AS highest_pump_apparent_power_kva,
    MIN(apparent_power_kva)
        AS lowest_pump_apparent_power_kva
FROM manpur_power_analytics.analytics.pump_demand;
-- ============================================================
CREATE OR REPLACE TABLE manpur_power_analytics.analytics.transformer_performance
USING DELTA
AS
SELECT
    element_name AS transformer_name,
    max_current_a,
    max_normal_loading_percent,
    max_emergency_loading_percent,
    max_apparent_power_kva,
    max_active_power_kw,
    max_reactive_power_kvar,
    loading_status,
    CASE
        WHEN max_normal_loading_percent > 100
            THEN 'Above Normal Rating'
        WHEN max_normal_loading_percent >= 80
            THEN 'High Utilization'
        WHEN max_normal_loading_percent IS NULL
            THEN 'Rating Not Available'
        ELSE 'Within Reported Rating'
    END AS transformer_capacity_status
FROM manpur_power_analytics.gold.asset_summary
WHERE asset_type = 'Transformer';
-- ============================================================
CREATE OR REPLACE TABLE manpur_power_analytics.analytics.voltage_unbalance_hotspots
USING DELTA
AS
SELECT
    ROW_NUMBER() OVER (
        ORDER BY voltage_unbalance_percent DESC
    ) AS unbalance_rank,
    bus_name,
    positive_sequence_voltage_v,
    voltage_pu,
    voltage_unbalance_percent,
    nema_unbalance_percent,
    unbalance_status
FROM manpur_power_analytics.gold.voltage_unbalance;
-- ============================================================
CREATE OR REPLACE TABLE manpur_power_analytics.analytics.management_kpi_summary
USING DELTA
AS
WITH voltage AS (
    SELECT
        COUNT(*) AS total_buses,
        SUM(
            CASE
                WHEN network_level = '415V_LT' THEN 1
                ELSE 0
            END
        ) AS lt_buses,
        SUM(
            CASE
                WHEN voltage_status = 'Low Voltage' THEN 1
                ELSE 0
            END
        ) AS low_voltage_buses,
        SUM(
            CASE
                WHEN voltage_status = 'Severe Deviation' THEN 1
                ELSE 0
            END
        ) AS severe_voltage_buses,
        MIN(voltage_deviation_percent)
            AS worst_voltage_deviation_percent,
        MIN_BY(
            bus_name,
            voltage_deviation_percent
        ) AS worst_voltage_bus
    FROM manpur_power_analytics.gold.voltage_analysis
),
deterioration AS (
    SELECT
        MAX(ABS(voltage_change_from_dt_side_percent))
            AS max_downstream_deterioration_percent,
        MIN_BY(
            bus_name,
            voltage_change_from_dt_side_percent
        ) AS greatest_deterioration_bus
    FROM manpur_power_analytics.analytics.voltage_deterioration
),
pumps AS (
    SELECT
        COUNT(*) AS pump_count,
        SUM(active_power_kw) AS total_pump_active_power_kw,
        SUM(reactive_power_kvar) AS total_pump_reactive_power_kvar,
        SUM(apparent_power_kva) AS total_pump_apparent_power_kva,
        AVG(power_factor) AS average_pump_power_factor,
        MAX(apparent_power_kva) AS highest_pump_apparent_power_kva
    FROM manpur_power_analytics.analytics.pump_demand
),
transformer AS (
    SELECT
        MAX(max_normal_loading_percent)
            AS max_transformer_normal_loading_percent,
        MAX_BY(
            transformer_name,
            max_normal_loading_percent
        ) AS highest_loaded_transformer
    FROM manpur_power_analytics.analytics.transformer_performance
),
unbalance AS (
    SELECT
        SUM(
            CASE
                WHEN unbalance_status = 'Moderate Unbalance'
                THEN 1 ELSE 0
            END
        ) AS moderate_unbalance_buses,
        SUM(
            CASE
                WHEN unbalance_status = 'Low Unbalance'
                THEN 1 ELSE 0
            END
        ) AS low_unbalance_buses
    FROM manpur_power_analytics.analytics.voltage_unbalance_hotspots
)
SELECT
    v.total_buses,
    v.lt_buses,
    v.low_voltage_buses,
    v.severe_voltage_buses,
    ROUND(v.worst_voltage_deviation_percent, 2)
        AS worst_voltage_deviation_percent,
    v.worst_voltage_bus,
    ROUND(d.max_downstream_deterioration_percent, 2)
        AS max_downstream_deterioration_percent,
    d.greatest_deterioration_bus,
    p.pump_count,
    ROUND(p.total_pump_active_power_kw, 2)
        AS total_pump_active_power_kw,
    ROUND(p.total_pump_reactive_power_kvar, 2)
        AS total_pump_reactive_power_kvar,
    ROUND(p.total_pump_apparent_power_kva, 2)
        AS total_pump_apparent_power_kva,
    ROUND(p.average_pump_power_factor, 3)
        AS average_pump_power_factor,
    ROUND(p.highest_pump_apparent_power_kva, 2)
        AS highest_pump_apparent_power_kva,
    ROUND(t.max_transformer_normal_loading_percent, 2)
        AS max_transformer_normal_loading_percent,
    t.highest_loaded_transformer,
    u.moderate_unbalance_buses,
    u.low_unbalance_buses
FROM voltage v
CROSS JOIN deterioration d
CROSS JOIN pumps p
CROSS JOIN transformer t
CROSS JOIN unbalance u;
SELECT *
FROM manpur_power_analytics.analytics.management_kpi_summary;

%sql
SELECT
    network_level,
    COUNT(*) AS bus_count
FROM manpur_power_analytics.gold.voltage_analysis
GROUP BY network_level
ORDER BY bus_count DESC;

%md

%md
Analytics 8 — FINAL VALIDATION

%sql
-- ============================================================
-- ANALYTICS 8 — FINAL VALIDATION
-- ============================================================
-- 1. Check all Analytics tables
SELECT
    'voltage_performance' AS table_name,
    COUNT(*) AS row_count
FROM manpur_power_analytics.analytics.voltage_performance
UNION ALL
SELECT
    'voltage_deterioration',
    COUNT(*)
FROM manpur_power_analytics.analytics.voltage_deterioration
UNION ALL
SELECT
    'asset_demand',
    COUNT(*)
FROM manpur_power_analytics.analytics.asset_demand
UNION ALL
SELECT
    'pump_demand',
    COUNT(*)
FROM manpur_power_analytics.analytics.pump_demand
UNION ALL
SELECT
    'pump_demand_contribution',
    COUNT(*)
FROM manpur_power_analytics.analytics.pump_demand_contribution
UNION ALL
SELECT
    'pump_demand_kpi',
    COUNT(*)
FROM manpur_power_analytics.analytics.pump_demand_kpi
UNION ALL
SELECT
    'transformer_performance',
    COUNT(*)
FROM manpur_power_analytics.analytics.transformer_performance
UNION ALL
SELECT
    'voltage_unbalance_hotspots',
    COUNT(*)
FROM manpur_power_analytics.analytics.voltage_unbalance_hotspots
UNION ALL
SELECT
    'management_kpi_summary',
    COUNT(*)
FROM manpur_power_analytics.analytics.management_kpi_summary;
-- ============================================================
-- 2. FINAL KPI CHECK
-- ============================================================
SELECT *
FROM manpur_power_analytics.analytics.management_kpi_summary;
-- ============================================================
-- 3. CHECK CRITICAL NULLS
-- ============================================================
SELECT
    SUM(CASE WHEN total_buses IS NULL THEN 1 ELSE 0 END)
        AS null_total_buses,
    SUM(CASE WHEN worst_voltage_bus IS NULL THEN 1 ELSE 0 END)
        AS null_worst_voltage_bus,
    SUM(CASE WHEN pump_count IS NULL THEN 1 ELSE 0 END)
        AS null_pump_count,
    SUM(CASE WHEN total_pump_active_power_kw IS NULL THEN 1 ELSE 0 END)
        AS null_total_pump_power,
    SUM(CASE WHEN highest_loaded_transformer IS NULL THEN 1 ELSE 0 END)
        AS null_transformer,
    SUM(CASE WHEN moderate_unbalance_buses IS NULL THEN 1 ELSE 0 END)
        AS null_unbalance
FROM manpur_power_analytics.analytics.management_kpi_summary;
