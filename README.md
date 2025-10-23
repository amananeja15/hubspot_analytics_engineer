HubSpot Analytics Engineer Technical Assessment

This project contains a dbt + Snowflake data model that transforms Airbnb-style listing data into analysis-ready tables to answer three business questions related to pricing, amenities, and booking behavior.

📂 Project Overview

The goal of this project is to ingest raw data into Snowflake, model it using dbt following a layered approach, and produce insights through clean and maintainable transformations.

The project uses a standard dbt architecture:

    Layer               	Purpose
    Staging	        Clean and standardize raw source data
    Intermediate	Apply business logic and aggregations
    Marts	        Final analytical tables for reporting & insights
    Analysis	    Queries that answer the business questions

🧠 Data Flow

RAW → STAGING → INTERMEDIATE → MARTS → ANALYSIS

Staging models: cleaned versions of raw Snowflake tables
Intermediate models: logic such as exploding amenities and aggregating reviews
Marts: dimension & fact tables used for BI and analysis

🏗️ Models Included

Staging

stg_listings
stg_calendar
stg_reviews
stg_amenities_changelog

Intermediate
int_amenities_exploded – one row per listing × change date × amenity
int_reviews_agg – review metrics per listing

Marts

dim_listings – core listing dimension with attributes, review metrics, and amenity flags
fct_calendar_daily – daily fact table with price, availability, reservations, and revenue

🧪 How to Run the Project

Requirements:

Snowflake account
dbt Cloud or dbt Core environment

Commands:

dbt deps
dbt build

This will install packages, run all models, and execute tests.

📊 Business Questions Answered

The analysis folder contains SQL queries that answer:

Revenue split by month based on AC availability

Average price increase by neighborhood (Jul 12, 2021 → Jul 11, 2022)

Maximum possible stay (overall and for listings with specific amenities)

🔍 Key Assumptions

Revenue = price only when a reservation exists

Latest amenities snapshot used for amenity flags

For price-change analysis, listings must have prices on both start & end dates

🛠️ Tech Stack

Snowflake — Data warehouse
dbt — Data modeling & transformation
GitHub — Version control and pull-request workflow

✅ Status

All models are tested, documented, and validated in Snowflake.
Project ready for review.