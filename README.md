# Airbnb Project:

## Table of Contents

- [1) Introduction](#1-introduction)
- [2) Datasets](#2-datasets)
- [3) Code Structure](#3-code-structure)
- [4) Orchestration](#4-orchestration)
- [5) User Guide](#5-user-guide)
  
## 1) Introduction:
![alt text](https://github.com/minhD03/airbnb-project/blob/60d73f852733d80819e778f56af06856d1d01d91/images/diagram2.png)


This project is my use of Dagster to build Data Orchestration, a cloud native data platform that handles from building, observing to delivering. Furthermore, this Data Orchestrator platform is utilized for the use of both human and AI agents to rely on. Unlike Apache Airflow, Dagster is focused more on data assets (instead of only tasks) with better local development, testing and debugging, followed by full supports for entire data life cycles. Therefore, Dagster is more suitable for modern data stack.

The datasets will be extracted from [Here](dbtlearn.s3.amazonaws.com) using dbt into my Snowflake Database in Raw schema for starter. Then, I will transform the datasets before visualizing it in Power BI.

## 2) Datasets:
![alt text](https://github.com/minhD03/airbnb-project/blob/60d73f852733d80819e778f56af06856d1d01d91/images/diagram1.png)

The datasets contains 4 files:
- Listings: List of Rooms for rents.
- Reviews: Reviews from customer.
- Hosts: The room host information.
- Full Moon Dates: The list of Full Moon days. I will use this to analyze if it affect the rent behaviour or not.

## 3) Code Structure:
```
.
├── docker-compose.yaml
├── airbnb_dagster/
│   └── airbnb_dagster          # manage the dag flow.
│        ├── assets.py          # asset the required condition.
│        ├── schedule.py        # schedule the materialization.
│        ├── project.py         # set up project path.
│        └── definitions.py     # main control file
├── airbnb_dbt/
│   ├── analyses                # my full moon analyze.
│   ├── models                  # main sql processing.
│   └── macros                  # check for anomalies
└── docker/                     # main docker set up.

```

## 4) Orchestration:
![alt text](https://github.com/minhD03/airbnb-project/blob/60d73f852733d80819e778f56af06856d1d01d91/images/pic12.png)

This is my pipeline used in this project. These are the actions that I have taken:
- After extracting datasets into my Snowflake Database called "airbnb_michael", I use SQL queries to convert the raw data into my source data by choosing the appropriate name for the columns.
- For Hosts, I added the "Anonymous" for any None Host names. For Listings, I set the minium nights to 1 and remove the dollar sign ($), followed by formating the price to decimal with maxium 10 digits totals, 2 digits after decimal. These tables are combined together with selected columns. This results in 1 dim table.
- For Reviews, I set the dataset as Incremental Materialization and use the surrogate key as review key for easier management. This results in 1 fact table.
- For Full Moon dates, I combine it with reviews and then analyze whether the reviewed data is full moon or not, which results in 1 mart table.
- Data materialization indicates the process of applying computation and save the result into a physical storage medium. There are 5 main types:
    - View: Calculate data upon request and does not store data. Suitable for staging data.
    - Tables: Physical dataset that is rebuilt every run. This provides the fastest retrieval but most computing consumption. I used this for dim datasets (cleaned datasets)
    - Incremental: Rebuild with only append or update new or changed data since the previous pipeline run. I used this for Fact Reviews as it will be updated by time.
    - Materialized Views: Database objects that use pre-computed queries to merge the real-time fressness of a view with fast read performance.
    - Emphemeral: Short-lived transformation scripts injected as Common Table Expressions (CTEs) into queries. These will never be written to database. After confirming my transformation pipeline worked correctly, I set the source queries to be emphemeral.
- The results can be seen in [Images](https://github.com/minhD03/airbnb-project/tree/60d73f852733d80819e778f56af06856d1d01d91/images)
  
## 5. User guide:

### a. Create user for dbt:
First, create:

- dbt user to grant the local dbt access permission to Snowflake database with permission to transform data.
- Database and schema to import data.
- Let user dbt bypass MFA for 24 hours.

```bash
USE ROLE ACCOUNTADMIN;
CREATE ROLE IF NOT EXISTS transform;
GRANT ROLE TRANSFORM TO ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;

CREATE USER IF NOT EXISTS dbt
  PASSWORD='dbtPassword123'
  LOGIN_NAME='dbt'
  MUST_CHANGE_PASSWORD=FALSE
  DEFAULT_WAREHOUSE='COMPUTE_WH'
  DEFAULT_ROLE='transform'
  DEFAULT_NAMESPACE='AIRBNB_MICHAEL.RAW'
  COMMENT='DBT user used for data transformation';
GRANT ROLE transform to USER dbt;
CREATE DATABASE IF NOT EXISTS AIRBNB_MICHAEL;
CREATE SCHEMA IF NOT EXISTS AIRBNB_MICHAEL.RAW;

GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE transform; 
GRANT ALL ON DATABASE AIRBNB_MICHAEL to ROLE transform;
GRANT ALL ON ALL SCHEMAS IN DATABASE AIRBNB_MICHAEL to ROLE transform;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE AIRBNB_MICHAEL to ROLE transform;
GRANT ALL ON ALL TABLES IN SCHEMA AIRBNB_MICHAEL.RAW to ROLE transform;
GRANT ALL ON FUTURE TABLES IN SCHEMA AIRBNB_MICHAEL.RAW to ROLE transform;
ALTER USER dbt SET MINS_TO_BYPASS_MFA = 1440;

```

### b. Setting up tables:
Then, continue with setting up tables inside the schema:

```bash
CREATE OR REPLACE TABLE raw_listings
                    (id integer,
                     listing_url string,
                     name string,
                     room_type string,
                     minimum_nights integer,
                     host_id integer,
                     price string,
                     created_at datetime,
                     updated_at datetime);
                    
COPY INTO raw_listings (id,
                        listing_url,
                        name,
                        room_type,
                        minimum_nights,
                        host_id,
                        price,
                        created_at,
                        updated_at)
                   from 's3://dbtlearn/listings.csv'
                    FILE_FORMAT = (type = 'CSV' skip_header = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"');
                    

CREATE OR REPLACE TABLE raw_reviews
                    (listing_id integer,
                     date datetime,
                     reviewer_name string,
                     comments string,
                     sentiment string);
                    
COPY INTO raw_reviews (listing_id, date, reviewer_name, comments, sentiment)
                   from 's3://dbtlearn/reviews.csv'
                    FILE_FORMAT = (type = 'CSV' skip_header = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"');
                    

CREATE OR REPLACE TABLE raw_hosts
                    (id integer,
                     name string,
                     is_superhost string,
                     created_at datetime,
                     updated_at datetime);
                    
COPY INTO raw_hosts (id, name, is_superhost, created_at, updated_at)
                   from 's3://dbtlearn/hosts.csv'
                    FILE_FORMAT = (type = 'CSV' skip_header = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"');

```

### c. For Power Bi users:
Create another role called Reporter for Power BI user to Load final processed datasets into Dashboard for visualization:
```bash
USE ROLE ACCOUNTADMIN;
CREATE ROLE IF NOT EXISTS REPORTER;
CREATE USER IF NOT EXISTS PBI
    PASSWORD='pbiPassword123'
    LOGIN_NAME='pbi'
    MUST_CHANGE_PASSWORD=FALSE
    DEFAULT_WAREHOUSE='COMPUTE_WH'
    DEFAULT_ROLE='REPORTER'
    DEFAULT_NAMESPACE='AIRBNB.DEV_MICHAEL'
    COMMENT='PowerBI user for creating reports';
GRANT ROLE REPORTER TO USER PBI;
GRANT ROLE REPORTER TO ROLE ACCOUNTADMIN;
GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE REPORTER;
GRANT USAGE ON DATABASE AIRBNB_MICHAEL TO ROLE REPORTER;
GRANT USAGE ON SCHEMA AIRBNB_MICHAEL.DEV_MICHAEL TO ROLE REPORTER;

```

## 5. Run the Orchestration:
To run using Docker Container:

- Install Git Bash and then create another file called ".env". This will be the file to contains your personal login information.
- Put the following information in the example:

```bash
SNOWFLAKE_ACCOUNT="your Snowflake account in your account url. Replace a/b into a-b"
SNOWFLAKE_DATABASE="database name in Snowflake"
SNOWFLAKE_PASSWORD="dbtPassword123"
SNOWFLAKE_ROLE="transform"
SNOWFLAKE_SCHEMA="your new schema name for dbt user to save processed data."
SNOWFLAKE_USER="dbt"
SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
DBT_TARGET="dev"
DAGSTER_POSTGRES_USER="postgres"
DAGSTER_POSTGRES_PASSWORD="postgres"
DAGSTER_POSTGRES_DB="postgres"
DAGSTER_CURRENT_IMAGE="dagster-codeserver"

```

- Then, save the set up using:
```bash
source .env
```

- And run docker compose:
```bash
docker compose up -d --build
```
- Once the container is initialized, head to http://localhost:3000/
- Go to Jobs => materialize_dbt_models => Materialize all.
- Wait for the process to me completed and see it inside Snowflake database.
- If you have error related to parsing macro files, open docker container. Execute these commands inside the dagster_codeserver to clean and reinstall dbt packages, then rerun the Materialize
```bash
cd airbnb_dbt
dbt clean
dbt deps
dbt compile
```
