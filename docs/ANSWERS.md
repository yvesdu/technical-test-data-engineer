# Réponses du test

## _Utilisation de la solution (étape 1 à 3)_

_Inscrire la documentation technique_

#### Technical Stack

FastAPI: API framework for data generation
Apache Airflow: ETL pipeline orchestration
DuckDB: Analytical database for data storage
Docker: Containerization and deployment
PostgreSQL: Airflow metadata storage

#### Implementation Details

Data Generation Layer (FastAPI)

Endpoints: /tracks, /users, /listen_history
Data formats: JSON responses with music listening data

#### ETL Pipeline (Airflow)

Daily data extraction from API
Parquet file storage for efficiency
DuckDB loading for analysis

#### Data Storage (DuckDB)

Dimensional model: dim_tracks, dim_users, fact_listen_history
Optimized for analytical queries
Parquet integration for efficient data loading

## Questions (étapes 4 à 7)

### Étape 4

#### DuckDB Selection Rationale

Column-oriented storage for analytical workloads
Direct Parquet file support
Simple deployment without separate server
SQL interface for easy querying
Low maintenance overhead

#### Database Schema Documentation for MooVitamix

#### Dimensional Model in DuckDB

#### Dimension Tables

CREATE TABLE dim_tracks (
track_id INTEGER,
name VARCHAR,
artist VARCHAR,
songwriters VARCHAR,
duration VARCHAR,
genres VARCHAR,
album VARCHAR,
created_at TIMESTAMP,
updated_at TIMESTAMP,
etl_updated_at TIMESTAMP,
PRIMARY KEY (track_id)
);

CREATE TABLE dim_users (
user_id INTEGER,
first_name VARCHAR,
last_name VARCHAR,
email VARCHAR,
gender VARCHAR,
favorite_genres VARCHAR,
created_at TIMESTAMP,
updated_at TIMESTAMP,
etl_updated_at TIMESTAMP,
PRIMARY KEY (user_id)
);

#### fact table

CREATE TABLE fact_listen_history (
user_id INTEGER,
track_id INTEGER,
listened_at TIMESTAMP,
load_date DATE
);

#### Design Choices

1. Tracking Fields

   created_at: Original record creation timestamp
   updated_at: Last update timestamp
   etl_updated_at: ETL process timestamp

2. Data Types

   INTEGER for IDs
   VARCHAR for text fields
   TIMESTAMP for temporal data

3. Relationships

   fact_listen_history links to both dimension tables via user_id and track_id

### Étape 5

#### Pipeline Monitoring

#### Technical Metrics

Pipeline execution time
Task success/failure rates
Data volume processed
API response times

#### Data Quality Metrics

Null value counts
Data freshness
Record counts
Schema validation

#### Monitoring Implementation

Airflow task-level monitoring
Data quality checks in DAG
Volume storage monitoring
API health checks

### Étape 6

#### Recommendation System Automation

#### Daily Pipeline

Feature extraction from user interactions
Batch prediction generation
Results caching in database
API endpoint for serving

##### Processing Steps

Extract recent user interactions
Generate user features
Apply current model
Store recommendations

### Étape 7

#### Model Retraining Automation with MLflow(I have previous experience with MLflow)

#### hourly/Daily/Weekly Pipeline

Historical data aggregation
Feature engineering
Model training with MLflow tracking
Performance evaluation
Model promotion if metrics improve

#### MLflow Integration

Version control for models
Metric tracking
Model registry
Automated deployment
