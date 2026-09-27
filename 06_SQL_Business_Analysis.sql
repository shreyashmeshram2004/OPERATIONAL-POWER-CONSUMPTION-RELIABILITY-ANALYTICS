-- Databricks notebook source
%md
Business Question 1 — Where is the voltage problem concentrated?

Management question:

Which buses have the largest voltage deviations, and how severe is the problem?



-- ============================================================
-- BUSINESS QUESTION 1
-- Where is the voltage problem concentrated?
-- ============================================================

SELECT
    bus_name,
    network_level,
    ROUND(voltage_average_v, 2) AS average_voltage_v,
    ROUND(average_voltage_pu, 4) AS voltage_pu,
    ROUND(voltage_deviation_percent, 2) AS voltage_deviation_percent,
    voltage_status
FROM manpur_power_analytics.analytics.voltage_performance
ORDER BY voltage_deviation_percent ASC;



-- ============================================================
-- VOLTAGE RISK LOCATIONS
-- ============================================================

SELECT
    bus_name,
    network_level,
    ROUND(voltage_average_v, 2) AS average_voltage_v,
    ROUND(voltage_deviation_percent, 2) AS voltage_deviation_percent,
    voltage_status,
    voltage_risk_rank
FROM manpur_power_analytics.analytics.voltage_performance
WHERE voltage_status IN (
    'Low Voltage',
    'Severe Deviation'
)
ORDER BY voltage_risk_rank;



%md
Business Question 2 — Which pumps contribute most to demand?



-- ============================================================
-- BUSINESS QUESTION 2
-- Which agricultural pumps contribute most to demand?
-- ============================================================

SELECT
    pump_name,
    ROUND(active_power_kw, 2) AS active_power_kw,
    ROUND(reactive_power_kvar, 2) AS reactive_power_kvar,
    ROUND(apparent_power_kva, 2) AS apparent_power_kva,
    ROUND(power_factor, 3) AS power_factor,
    ROUND(apparent_power_contribution_percent, 2)
        AS apparent_power_contribution_percent
FROM manpur_power_analytics.analytics.pump_demand_contribution
ORDER BY apparent_power_kva DESC;



SELECT
    pump_name,
    ROUND(apparent_power_kva, 2) AS apparent_power_kva,
    ROUND(apparent_power_contribution_percent, 2)
        AS contribution_percent
FROM manpur_power_analytics.analytics.pump_demand_contribution
ORDER BY apparent_power_kva DESC
LIMIT 5;



%md
Business Question 3 — Which network assets carry the greatest demand?



-- ============================================================
-- BUSINESS QUESTION 3
-- Which assets carry the greatest demand?
-- ============================================================

SELECT
    demand_rank,
    element_name,
    asset_type,
    ROUND(apparent_power_kva, 2) AS apparent_power_kva,
    ROUND(active_power_kw, 2) AS active_power_kw,
    ROUND(reactive_power_kvar, 2) AS reactive_power_kvar,
    ROUND(power_factor, 3) AS power_factor

FROM manpur_power_analytics.analytics.asset_demand

ORDER BY apparent_power_kva DESC;



-- ============================================================
-- TOP 10 ASSETS BY APPARENT POWER
-- ============================================================

SELECT
    demand_rank,
    element_name,
    asset_type,
    ROUND(apparent_power_kva, 2) AS apparent_power_kva,
    ROUND(active_power_kw, 2) AS active_power_kw,
    ROUND(reactive_power_kvar, 2) AS reactive_power_kvar

FROM manpur_power_analytics.analytics.asset_demand

ORDER BY apparent_power_kva DESC

LIMIT 10;



%md
Business Question 5 — Which transformer shows capacity/loading risk?



-- ============================================================
-- BUSINESS QUESTION 5
-- Transformer capacity/loading assessment
-- ============================================================

SELECT
    transformer_name,
    ROUND(max_current_a, 2) AS max_current_a,
    ROUND(max_normal_loading_percent, 2)
        AS normal_loading_percent,
    ROUND(max_emergency_loading_percent, 2)
        AS emergency_loading_percent,
    ROUND(max_apparent_power_kva, 2)
        AS apparent_power_kva,
    loading_status,
    transformer_capacity_status

FROM manpur_power_analytics.analytics.transformer_performance

ORDER BY max_normal_loading_percent DESC;



%md
Business Question 6 — Where are power-quality hotspots?



-- ============================================================
-- BUSINESS QUESTION 6
-- Voltage unbalance hotspots
-- ============================================================

SELECT
    unbalance_rank,
    bus_name,
    ROUND(positive_sequence_voltage_v, 2)
        AS positive_sequence_voltage_v,
    ROUND(voltage_unbalance_percent, 3)
        AS voltage_unbalance_percent,
    ROUND(nema_unbalance_percent, 3)
        AS nema_unbalance_percent,
    unbalance_status

FROM manpur_power_analytics.analytics.voltage_unbalance_hotspots

ORDER BY unbalance_rank;



SELECT
    bus_name,
    ROUND(voltage_unbalance_percent, 3)
        AS voltage_unbalance_percent,
    ROUND(nema_unbalance_percent, 3)
        AS nema_unbalance_percent
FROM manpur_power_analytics.analytics.voltage_unbalance_hotspots
WHERE unbalance_status = 'Moderate Unbalance'
ORDER BY voltage_unbalance_percent DESC;



%md
Business Question 7 — What does the overall network look like?



-- ============================================================
-- BUSINESS QUESTION 7
-- Executive network snapshot
-- ============================================================

SELECT
    total_buses,
    lt_buses,
    low_voltage_buses,
    severe_voltage_buses,

    worst_voltage_bus,
    worst_voltage_deviation_percent,

    greatest_deterioration_bus,
    max_downstream_deterioration_percent,

    pump_count,
    total_pump_active_power_kw,
    total_pump_apparent_power_kva,
    average_pump_power_factor,

    highest_loaded_transformer,
    max_transformer_normal_loading_percent,

    moderate_unbalance_buses,
    low_unbalance_buses

FROM manpur_power_analytics.analytics.management_kpi_summary;



%md
Business Question 8 — Can we identify priority locations?



-- ============================================================
-- BUSINESS QUESTION 8
-- Identify locations requiring analytical attention
-- ============================================================

SELECT
    v.bus_name,
    v.network_level,

    ROUND(v.voltage_deviation_percent, 2)
        AS voltage_deviation_percent,

    ROUND(d.voltage_change_from_dt_side_percent, 2)
        AS downstream_change_percent,

    ROUND(u.voltage_unbalance_percent, 3)
        AS voltage_unbalance_percent,

    v.voltage_status,
    u.unbalance_status

FROM manpur_power_analytics.analytics.voltage_performance v

LEFT JOIN manpur_power_analytics.analytics.voltage_deterioration d
    ON v.bus_name = d.bus_name

LEFT JOIN manpur_power_analytics.analytics.voltage_unbalance_hotspots u
    ON v.bus_name = u.bus_name

WHERE v.network_level = '415V_LT'

ORDER BY
    v.voltage_deviation_percent ASC;



%md
06_SQL_Business_Analysis

01. Voltage problem concentration       ← Q1
02. Pump demand contribution            ← Q2
03. Asset demand                       ← Q3
04. Downstream deterioration            ← Q4
05. Transformer capacity/loading       ← Q5
06. Voltage unbalance hotspots         ← Q6
07. Executive network snapshot         ← Q7
08. Priority-location analysis         ← Q8
