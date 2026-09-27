-- Databricks notebook source
%md
06_PowerBI_Preparation — Cell 1



-- ============================================================
-- 06_POWERBI_PREPARATION
-- Create Power BI-ready datasets
-- ============================================================

-- 1. NETWORK HEALTH
CREATE OR REPLACE TABLE manpur_power_analytics.analytics.powerbi_network_health
USING DELTA AS

SELECT
    v.bus_name,
    v.network_level,
    ROUND(v.nominal_voltage_kv, 3) AS nominal_voltage_kv,
    ROUND(v.voltage_average_v, 2) AS average_voltage_v,
    ROUND(v.average_voltage_pu, 4) AS voltage_pu,
    ROUND(v.voltage_deviation_percent, 2) AS voltage_deviation_percent,
    ROUND(v.phase_voltage_spread_v, 2) AS phase_voltage_spread_v,
    v.voltage_status,

    ROUND(d.voltage_change_from_dt_side_v, 2)
        AS downstream_voltage_change_v,

    ROUND(d.voltage_change_from_dt_side_percent, 2)
        AS downstream_voltage_change_percent,

    ROUND(u.voltage_unbalance_percent, 3)
        AS voltage_unbalance_percent,

    ROUND(u.nema_unbalance_percent, 3)
        AS nema_unbalance_percent,

    u.unbalance_status

FROM manpur_power_analytics.analytics.voltage_performance v

LEFT JOIN manpur_power_analytics.analytics.voltage_deterioration d
    ON v.bus_name = d.bus_name

LEFT JOIN manpur_power_analytics.analytics.voltage_unbalance_hotspots u
    ON v.bus_name = u.bus_name;


-- ============================================================
-- 2. PUMP DEMAND
-- ============================================================

CREATE OR REPLACE TABLE manpur_power_analytics.analytics.powerbi_pump_demand
USING DELTA AS

SELECT
    pump_name,
    ROUND(active_power_kw, 2) AS active_power_kw,
    ROUND(reactive_power_kvar, 2) AS reactive_power_kvar,
    ROUND(apparent_power_kva, 2) AS apparent_power_kva,
    ROUND(power_factor, 3) AS power_factor,
    ROUND(apparent_power_contribution_percent, 2)
        AS demand_contribution_percent,
    ROUND(cumulative_apparent_power_percent, 2)
        AS cumulative_demand_contribution_percent

FROM manpur_power_analytics.analytics.pump_demand_contribution;


-- 3. ASSET PERFORMANCE
CREATE OR REPLACE TABLE manpur_power_analytics.analytics.powerbi_asset_performance
USING DELTA AS

SELECT
    element_name,
    asset_type,
    ROUND(max_current_a, 2) AS max_current_a,
    ROUND(max_normal_loading_percent, 2)
        AS normal_loading_percent,
    ROUND(max_emergency_loading_percent, 2)
        AS emergency_loading_percent,
    ROUND(max_apparent_power_kva, 2)
        AS apparent_power_kva,
    ROUND(max_active_power_kw, 2) AS active_power_kw,
    ROUND(max_reactive_power_kvar, 2) AS reactive_power_kvar,
    loading_status

FROM manpur_power_analytics.gold.asset_summary;


-- 4. KPI SUMMARY
CREATE OR REPLACE TABLE manpur_power_analytics.analytics.powerbi_kpi_summary
USING DELTA AS

SELECT
    total_buses,
    lt_buses,
    low_voltage_buses,
    severe_voltage_buses,
    worst_voltage_bus,
    ROUND(worst_voltage_deviation_percent, 2)
        AS worst_voltage_deviation_percent,
    greatest_deterioration_bus,
    ROUND(max_downstream_deterioration_percent, 2)
        AS max_downstream_deterioration_percent,
    pump_count,
    ROUND(total_pump_active_power_kw, 2)
        AS total_pump_active_power_kw,
    ROUND(total_pump_apparent_power_kva, 2)
        AS total_pump_apparent_power_kva,
    ROUND(average_pump_power_factor, 3)
        AS average_pump_power_factor,
    highest_loaded_transformer,
    ROUND(max_transformer_normal_loading_percent, 2)
        AS max_transformer_normal_loading_percent,
    moderate_unbalance_buses,
    low_unbalance_buses

FROM manpur_power_analytics.analytics.management_kpi_summary;


-- 5. PRIORITY LOCATIONS
CREATE OR REPLACE TABLE manpur_power_analytics.analytics.powerbi_priority_locations
USING DELTA AS

SELECT
    v.bus_name,
    v.network_level,

    ROUND(v.voltage_average_v, 2)
        AS average_voltage_v,

    ROUND(v.voltage_deviation_percent, 2)
        AS voltage_deviation_percent,

    v.voltage_status,

    ROUND(d.voltage_change_from_dt_side_percent, 2)
        AS downstream_voltage_change_percent,

    ROUND(u.voltage_unbalance_percent, 3)
        AS voltage_unbalance_percent,

    u.unbalance_status

FROM manpur_power_analytics.analytics.voltage_performance v

LEFT JOIN manpur_power_analytics.analytics.voltage_deterioration d
    ON v.bus_name = d.bus_name

LEFT JOIN manpur_power_analytics.analytics.voltage_unbalance_hotspots u
    ON v.bus_name = u.bus_name

WHERE v.network_level = '415V_LT';



%md
Then Cell 2 — validation



-- ============================================================
-- POWER BI PREPARATION VALIDATION
-- ============================================================

SELECT 'powerbi_network_health' AS table_name,
       COUNT(*) AS row_count
FROM manpur_power_analytics.analytics.powerbi_network_health

UNION ALL

SELECT 'powerbi_pump_demand',
       COUNT(*)
FROM manpur_power_analytics.analytics.powerbi_pump_demand

UNION ALL

SELECT 'powerbi_asset_performance',
       COUNT(*)
FROM manpur_power_analytics.analytics.powerbi_asset_performance

UNION ALL

SELECT 'powerbi_kpi_summary',
       COUNT(*)
FROM manpur_power_analytics.analytics.powerbi_kpi_summary

UNION ALL

SELECT 'powerbi_priority_locations',
       COUNT(*)
FROM manpur_power_analytics.analytics.powerbi_priority_locations;



%md
final schema/content sanity check



-- ============================================================
-- FINAL POWER BI DATASET CHECK
-- ============================================================

SELECT
    'network_health' AS dataset,
    COUNT(*) AS rows,
    COUNT(DISTINCT bus_name) AS unique_business_entities
FROM manpur_power_analytics.analytics.powerbi_network_health

UNION ALL

SELECT
    'pump_demand',
    COUNT(*),
    COUNT(DISTINCT pump_name)
FROM manpur_power_analytics.analytics.powerbi_pump_demand

UNION ALL

SELECT
    'asset_performance',
    COUNT(*),
    COUNT(DISTINCT element_name)
FROM manpur_power_analytics.analytics.powerbi_asset_performance

UNION ALL

SELECT
    'priority_locations',
    COUNT(*),
    COUNT(DISTINCT bus_name)
FROM manpur_power_analytics.analytics.powerbi_priority_locations;
