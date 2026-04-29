-- Create PostgreSQL analytics database and tables
CREATE DATABASE IF NOT EXISTS flight_price_analysis;
\c flight_price_analysis;

-- Table for storing KPIs
CREATE TABLE IF NOT EXISTS flight_price_kpis (
    airline VARCHAR(255),
    avg_fare DECIMAL(10,2),
    seasonal_fare_variation DECIMAL(10,2)
);

-- Indexes to speed up analytics queries
CREATE INDEX idx_airline ON flight_price_kpis(airline);