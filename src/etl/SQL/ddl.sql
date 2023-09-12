-- DROP DATABASE IF EXISTS incidents
-- DROP TABLE IF EXISTS incidents.traffic_incidents
-- DROP TABLE IF EXISTS incidents.traffic_incidents_report

CREATE DATABASE IF NOT EXISTS incidents;

CREATE TABLE IF NOT EXISTS incidents.traffic_incidents 
(
    traffic_report_id String,
    published_date DateTime,
    issue_reported String,
    latitude Float64,
    longitude Float64,
    address String,
    traffic_report_status String,
    traffic_report_status_date_time DateTime,
    year UInt32,
    month String
) 
ENGINE = ReplacingMergeTree
ORDER BY traffic_report_id;

CREATE TABLE IF NOT EXISTS incidents.traffic_incidents_report (
    report_id UInt64,
    published_date DateTime,
    issue_reported String,
    latitude Float64,
    longitude Float64,
    address String,
    report_status String,
    report_status_date_time DateTime,
    year UInt32,
    month String
) 
ENGINE = ReplacingMergeTree(report_id) 
PRIMARY KEY report_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS incidents.report_incidents TO incidents.traffic_incidents_report 
AS 
    SELECT 
        reinterpretAsInt64(splitByString('_', traffic_report_id)[1]) AS report_id,
        published_date,
        issue_reported,
        latitude,
        longitude,
        address,
        traffic_report_status as report_status,
        traffic_report_status_date_time as report_status_date_time,
        year,
        month
    FROM incidents.traffic_incidents