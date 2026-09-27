-- Manpur Power Analytics
-- Project setup

CREATE CATALOG IF NOT EXISTS manpur_power_analytics;
USE CATALOG manpur_power_analytics;

CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Raw and minimally transformed source data';

CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Cleaned, standardized and validated analytical data';

CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Business-ready analytical data and KPIs';

CREATE SCHEMA IF NOT EXISTS analytics
COMMENT 'Final datasets for SQL analysis and Power BI';
