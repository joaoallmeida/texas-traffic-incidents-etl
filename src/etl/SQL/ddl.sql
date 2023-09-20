DROP DATABASE IF EXISTS incidents;

CREATE DATABASE IF NOT EXISTS incidents;

CREATE TABLE IF NOT EXISTS incidents.traffic_incidents_report
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
ENGINE = MergeTree()
ORDER BY traffic_report_id;