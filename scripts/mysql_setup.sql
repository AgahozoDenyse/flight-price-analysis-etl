-- Create MySQL staging database and table for flight price data
CREATE DATABASE IF NOT EXISTS flight_price_analysis;
USE flight_price_analysis;

-- Staging table for raw data
CREATE TABLE IF NOT EXISTS flight_price_staging (
    airline VARCHAR(255),
    source VARCHAR(255),
    destination VARCHAR(255),
    base_fare DECIMAL(10,2),
    tax_and_surcharge DECIMAL(10,2),
    total_fare DECIMAL(10,2)
);

-- Create indexes for optimization
CREATE INDEX idx_airline ON flight_price_staging(airline);
CREATE INDEX idx_source ON flight_price_staging(source);
CREATE INDEX idx_destination ON flight_price_staging(destination);