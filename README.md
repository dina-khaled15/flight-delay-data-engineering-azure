
````markdown
# Flight Delay Analysis & Analytics Platform

## 1. Project Overview

This project is an end-to-end data engineering and analytics solution designed to analyze historical flight data from 2009 to 2018.

The project processes raw flight data through a Medallion Architecture using Azure Data Lake Storage Gen2 and Azure Synapse Analytics. PySpark is used for data cleaning, transformation, feature engineering, and dimensional modeling.

The final solution transforms raw flight records into an analytical Gold layer and a Star Schema data model that can be consumed by Power BI for business intelligence and reporting.

The project focuses on understanding flight delays, evaluating airline and route performance, analyzing airport congestion, identifying time-based delay patterns, and investigating the root causes of delays.

---

## 2. Business Objectives

### 2.1 Flight Delay Analysis

Analyze historical flight data to identify delay patterns and trends.

Key metrics include:

- Average departure delay
- Average arrival delay
- Delay frequency
- Delay rate
- Delay severity
- Distribution of delays across airlines, routes, airports, and time periods

### 2.2 Airline Performance Evaluation

Evaluate airline punctuality and operational performance using On-Time Performance (OTP).

The analysis can compare airlines based on:

- On-time percentage
- Delay rate
- Average arrival delay
- Number of delayed flights

### 2.3 Route Performance Analysis

Evaluate the performance and reliability of flight routes.

The analysis focuses on:

- Delay rate by route
- Average delay by route
- Route reliability
- Flight distance
- Flight duration
- Short-, medium-, and long-haul routes

### 2.4 Airport Congestion Analysis

Analyze airport traffic and delay impact.

The analysis can identify:

- High-traffic airports
- Number of flights per airport
- Airport delay rates
- Average airport delay
- Airports associated with high levels of delays

### 2.5 Time-Based Delay Pattern Analysis

Analyze how delays change over time using:

- Year
- Month
- Day
- Day of week
- Departure hour
- Departure time bucket
- Weekend vs weekday

This helps identify peak delay periods and recurring temporal patterns.

### 2.6 Root Cause Analysis

Identify the major operational causes of flight delays.

The analysis considers:

- Carrier delays
- Weather delays
- NAS delays
- Security delays
- Late aircraft delays

---

## 3. Data Architecture

The project follows the Medallion Architecture:

```text
Raw Flight Data
       |
       v
+----------------+
| Bronze Layer   |
| Raw Data       |
+----------------+
       |
       v
+----------------+
| Silver Layer   |
| Cleaned Data   |
+----------------+
       |
       v
+----------------+
| Gold Layer     |
| Analytical     |
| Data           |
+----------------+
       |
       v
+----------------+
| Star Schema    |
| Data Model     |
+----------------+
       |
       v
+----------------+
| Power BI       |
| Analytics      |
+----------------+
````

---

## 4. Technologies Used

* Python
* PySpark
* Apache Spark
* Azure Synapse Analytics
* Azure Data Lake Storage Gen2
* Parquet
* CSV
* Power BI
* GitHub

---

# 5. Bronze Layer

The Bronze layer stores the raw flight data without applying business transformations.

The source dataset contains historical flight records from 2009 to 2018.

### Main responsibilities

* Store raw source data
* Preserve the original dataset
* Provide input for downstream processing
* Maintain a reliable source layer for data transformation

---

# 6. Silver Layer

The Silver layer contains cleaned and standardized flight data.

The main purpose of this layer is to improve data quality and prepare the data for analytical processing.

## 6.1 Loading Raw Data

The raw CSV files are loaded from the Bronze layer using PySpark.

```python
df_raw = spark.read \
    .option("header", "true") \
    .option("nullValue", "") \
    .option("mode", "PERMISSIVE") \
    .csv(RAW_PATH)
```

The data is loaded with headers and permissive parsing to handle invalid records without stopping the entire ingestion process.

---

## 6.2 Removing Unnecessary Columns

The column `Unnamed: 27` was removed because it does not contain useful business information and represents a noisy column in the source dataset.

```python
df = df_raw.drop("Unnamed: 27")
```

---

## 6.3 Removing Duplicate Flights

Duplicate records were removed using a set of business keys:

```text
FL_DATE
OP_CARRIER
OP_CARRIER_FL_NUM
ORIGIN
DEST
CRS_DEP_TIME
```

```python
df = df.dropDuplicates([
    "FL_DATE",
    "OP_CARRIER",
    "OP_CARRIER_FL_NUM",
    "ORIGIN",
    "DEST",
    "CRS_DEP_TIME"
])
```

This prevents the same flight from being counted multiple times during analysis.

---

## 6.4 Separating Operated and Cancelled Flights

The dataset was divided into operated and cancelled flights.

```python
df_cancelled = df.filter(F.col("CANCELLED") == 1)

df_operated = df.filter(F.col("CANCELLED") == 0)
```

The operated-flight dataset is used for the main delay analysis because operated flights contain actual departure and arrival performance information.

Cancelled flights are kept separately for potential cancellation analysis.

---

## 6.5 Handling Invalid Values

Invalid values such as:

```text
undefined
NA
null
NULL
empty strings
```

are treated as missing values.

This prevents invalid text values from affecting numerical calculations and analytical results.

---

## 6.6 Numeric Type Conversion

The following delay-related columns are converted to numeric types:

```text
DEP_DELAY
ARR_DELAY
CARRIER_DELAY
WEATHER_DELAY
NAS_DELAY
SECURITY_DELAY
LATE_AIRCRAFT_DELAY
```

This allows numerical calculations such as averages, totals, and comparisons.

---

## 6.7 Handling Missing Delay Values

Missing delay values are replaced with zero where appropriate for delay calculations.

This allows delay metrics and delay-cause calculations to be performed consistently.

---

## 6.8 Removing Records with Missing Identifiers

Records missing essential fields are removed:

```text
FL_DATE
OP_CARRIER
ORIGIN
DEST
```

These fields are required because they identify the flight, airline, origin, destination, and date.

---

## 6.9 Standardizing Data Formats

The following transformations are applied:

* Convert `FL_DATE` to a date type
* Convert `CANCELLED` to integer
* Convert `DIVERTED` to integer
* Convert carrier codes to uppercase
* Convert airport codes to uppercase
* Remove unnecessary whitespace

This improves consistency across the dataset.

---

## 6.10 Handling Extreme Delay Values

Extremely large delay values are capped at 1,440 minutes.

```python
df_operated = df_operated \
    .withColumn(
        "ARR_DELAY",
        F.when(F.col("ARR_DELAY") > 1440, 1440)
         .otherwise(F.col("ARR_DELAY"))
    ) \
    .withColumn(
        "DEP_DELAY",
        F.when(F.col("DEP_DELAY") > 1440, 1440)
         .otherwise(F.col("DEP_DELAY"))
    )
```

This is a data quality control step used to reduce the impact of extreme outliers.

---

## 6.11 Handling Invalid Air Time

Negative air-time values are invalid and therefore converted to NULL.

```python
df_operated = df_operated.withColumn(
    "AIR_TIME",
    F.when(F.col("AIR_TIME") < 0, None)
     .otherwise(F.col("AIR_TIME"))
)
```

---

## 6.12 Creating the Route Feature

A route identifier is created by combining the origin and destination airports.

```python
df_operated = df_operated.withColumn(
    "ROUTE",
    F.concat_ws("-", "ORIGIN", "DEST")
)
```

Example:

```text
JFK-LAX
ATL-ORD
LAX-SFO
```

The `ROUTE` feature is used for route performance and delay analysis.

---

## 6.13 Creating the Year Partition

The year is extracted from the flight date.

```python
df_operated = df_operated.withColumn(
    "YEAR",
    F.year("FL_DATE")
)
```

The data is partitioned by `YEAR` when written to the Silver layer.

Partitioning improves data organization and can improve query performance when filtering by year.

---

# 7. Gold Layer

The Gold layer contains analysis-ready flight data.

The Silver operated-flight data is transformed into a business-oriented analytical dataset.

---

## 7.1 Time Features

The following features are created:

```text
YEAR
MONTH
DAY
DAY_OF_WEEK
```

These columns allow the project to analyze delay patterns across different time periods.

For example:

* `YEAR` → yearly delay trends
* `MONTH` → seasonal patterns
* `DAY` → daily analysis
* `DAY_OF_WEEK` → weekday/weekend patterns

---

## 7.2 Departure Time Features

The scheduled departure time is transformed into:

```text
DEP_HOUR
```

This allows analysis by departure hour.

A second feature is created:

```text
DEP_TIME_BUCKET
```

with the following categories:

```text
Morning
Afternoon
Evening
Night
```

This helps identify periods during the day where delays are more frequent.

---

# 8. Delay Classification

## 8.1 IS_DELAYED

A flight is considered delayed when:

```text
ARR_DELAY >= 15 minutes
```

The resulting column contains:

```text
1 = Delayed
0 = Not Delayed
```

This column is used to calculate delay rates and identify delayed flights.

---

## 8.2 OTP_FLAG

The On-Time Performance flag is calculated using:

```text
ARR_DELAY <= 15 minutes
```

The resulting column contains:

```text
1 = On Time
0 = Delayed
```

This metric is used to evaluate airline and route punctuality.

---

# 9. Delay Severity

The `DELAY_SEVERITY` feature categorizes flights according to arrival delay.

Categories include:

```text
Early
On Time
Minor
Moderate
Severe
```

This allows the analysis to distinguish between small delays and severe operational disruptions.

---

# 10. Route and Distance Features

## 10.1 ROUTE

The route is created using:

```text
ORIGIN + DEST
```

Example:

```text
JFK-LAX
```

The route is used for route-level performance analysis.

---

## 10.2 HAUL_TYPE

Flight distance is categorized into:

```text
Short
Medium
Long
```

This enables comparison of delay behavior across different flight distances.

---

# 11. Delay Cause Analysis

The original dataset contains individual delay components:

```text
CARRIER_DELAY
WEATHER_DELAY
NAS_DELAY
SECURITY_DELAY
LATE_AIRCRAFT_DELAY
```

These columns represent the amount of delay attributed to different operational causes.

---

## 11.1 MAX_DELAY

The maximum delay contribution is calculated across all delay-cause columns.

This allows the project to identify the dominant cause of delay for each flight.

---

## 11.2 DELAY_CAUSE

The dominant delay cause is classified as:

```text
No Delay
Carrier
Weather
NAS
Security
Late Aircraft
Unknown
```

The classification is based on the delay component with the highest value.

This feature directly supports the root-cause analysis objective.

---

## 11.3 TOTAL_CAUSE_DELAY

The individual delay components are summed:

```text
CARRIER_DELAY
+
WEATHER_DELAY
+
NAS_DELAY
+
SECURITY_DELAY
+
LATE_AIRCRAFT_DELAY
```

The result is stored in:

```text
TOTAL_CAUSE_DELAY
```

This provides an overall measure of delay attributed to identified causes.

---

# 12. Gold Dataset Main Columns

The Gold dataset contains the original flight attributes plus analytical features.

### Flight Identification

```text
FL_DATE
OP_CARRIER
OP_CARRIER_FL_NUM
```

These identify the date, airline, and flight number.

### Airport Information

```text
ORIGIN
DEST
```

These identify the departure and destination airports.

They support airport and route analysis.

### Scheduled and Actual Times

```text
CRS_DEP_TIME
DEP_TIME
CRS_ARR_TIME
ARR_TIME
```

These support time-based analysis and operational performance analysis.

### Delay Metrics

```text
DEP_DELAY
ARR_DELAY
```

These are the main delay measures.

### Operational Metrics

```text
TAXI_OUT
TAXI_IN
AIR_TIME
DISTANCE
```

These help analyze flight efficiency and operational performance.

### Delay Causes

```text
CARRIER_DELAY
WEATHER_DELAY
NAS_DELAY
SECURITY_DELAY
LATE_AIRCRAFT_DELAY
```

These support root-cause analysis.

### Analytical Features

```text
YEAR
MONTH
DAY
DAY_OF_WEEK
DEP_HOUR
DEP_TIME_BUCKET
ROUTE
HAUL_TYPE
IS_DELAYED
OTP_FLAG
DELAY_SEVERITY
DELAY_CAUSE
TOTAL_CAUSE_DELAY
```

These features are used directly for analytical reporting and dashboard development.

---

# 13. Dimensional Modeling

The Gold layer is transformed into a Star Schema.

The purpose of dimensional modeling is to separate:

* Business events and measurable values
* Descriptive attributes used to analyze those events

The resulting model consists of one central fact table surrounded by dimension tables.

---

# 14. Star Schema

```text
                         +----------------+
                         |    Dim_Date    |
                         |----------------|
                         | DateKey        |
                         | FL_DATE        |
                         | Year           |
                         | Month          |
                         | Day            |
                         | DayOfWeek      |
                         | IsWeekend      |
                         +-------+--------+
                                 |
                                 |
+----------------+       +-------+--------+       +----------------+
|  Dim_Airline   |       |  Fact_Flight   |       |   Dim_Route    |
|----------------|       |----------------|       |----------------|
| CarrierKey     |------>| DateKey        |<------| RouteKey       |
| OP_CARRIER     |       | CarrierKey     |       | ROUTE          |
+----------------+       | RouteKey       |       | DISTANCE       |
                         | CauseKey       |       | HAUL_TYPE      |
                         | ORIGIN         |       +----------------+
                         | DEST           |
                         | DEP_DELAY      |
                         | ARR_DELAY      |
                         | DISTANCE       |
                         | AIR_TIME       |
                         | IS_DELAYED     |
                         | OTP_FLAG       |
                         | TOTAL_CAUSE_DELAY
                         +-------+--------+
                                 |
                                 |
                         +-------+--------+
                         | Dim_DelayCause|
                         |----------------|
                         | CauseKey       |
                         | DELAY_CAUSE    |
                         +----------------+

                         +----------------+
                         |  Dim_Airport   |
                         |----------------|
                         | AirportKey     |
                         | AIRPORT        |
                         +----------------+
```

---

# 15. Dimension Tables

## 15.1 Dim_Date

The date dimension is generated from the distinct flight dates.

Columns:

```text
DateKey
FL_DATE
Year
Month
Day
DayOfWeek
IsWeekend
```

### Why these columns?

`DateKey` is a surrogate key used to identify the date dimension record.

`FL_DATE` represents the actual flight date.

`Year` supports yearly trend analysis.

`Month` supports monthly and seasonal analysis.

`Day` supports daily analysis.

`DayOfWeek` supports weekday pattern analysis.

`IsWeekend` allows comparison between weekdays and weekends.

---

## 15.2 Dim_Airline

Columns:

```text
CarrierKey
OP_CARRIER
```

`CarrierKey` is the surrogate key.

`OP_CARRIER` identifies the operating airline.

This dimension supports airline performance evaluation.

---

## 15.3 Dim_Airport

The airport dimension combines airports appearing in both:

```text
ORIGIN
DEST
```

The `union()` operation combines both sets of airport codes.

Duplicates are then removed using:

```python
.distinct()
```

Columns:

```text
AirportKey
AIRPORT
```

This dimension supports airport traffic and congestion analysis.

---

## 15.4 Dim_Route

Columns:

```text
RouteKey
ROUTE
DISTANCE
HAUL_TYPE
```

`RouteKey` is the surrogate key.

`ROUTE` identifies the flight route.

`DISTANCE` represents the distance between origin and destination.

`HAUL_TYPE` categorizes the route as short, medium, or long haul.

This dimension supports route performance analysis.

---

## 15.5 Dim_DelayCause

Columns:

```text
CauseKey
DELAY_CAUSE
```

`CauseKey` is the surrogate key.

`DELAY_CAUSE` represents the dominant cause of delay.

This dimension supports root-cause analysis.

---

# 16. Surrogate Keys

Surrogate keys are generated for each dimension using Spark's `row_number()` function.

Examples:

```text
DateKey
CarrierKey
AirportKey
RouteKey
CauseKey
```

The surrogate keys provide unique numeric identifiers for dimension records.

They are used to connect the fact table with the dimensions.

---

# 17. Fact Table

The central table is:

```text
Fact_Flight
```

It represents individual flight events.

The fact table contains foreign keys referencing the dimensions and measurable analytical attributes.

Main columns:

```text
DateKey
CarrierKey
RouteKey
CauseKey
ORIGIN
DEST
DEP_DELAY
ARR_DELAY
DISTANCE
AIR_TIME
IS_DELAYED
OTP_FLAG
TOTAL_CAUSE_DELAY
```

---

# 18. Fact Table Design

The fact table was designed around the business event:

```text
One operated flight
```

Each row represents one operated flight.

The fact table contains:

### Foreign Keys

```text
DateKey
CarrierKey
RouteKey
CauseKey
```

These connect the fact table to the dimension tables.

### Measures

```text
DEP_DELAY
ARR_DELAY
DISTANCE
AIR_TIME
TOTAL_CAUSE_DELAY
```

These are numerical values that can be aggregated and analyzed.

### Flags

```text
IS_DELAYED
OTP_FLAG
```

These support delay-rate and OTP calculations.

### Airport Attributes

```text
ORIGIN
DEST
```

These support airport-level analysis.

---

# 19. Joining Fact and Dimensions

The dimension keys are added to the fact table by joining the fact data with the dimension tables.

Example:

```python
fact = fact \
    .join(dim_date, "FL_DATE", "left") \
    .join(dim_airline, "OP_CARRIER", "left") \
    .join(dim_route, "ROUTE", "left") \
    .join(dim_cause, "DELAY_CAUSE", "left")
```

A `LEFT JOIN` is used so that every flight record from the fact dataset is preserved even if a corresponding dimension record is missing.

The dimension keys are then selected into the final fact table.

---

# 20. Power BI Integration

The Star Schema can be connected to Power BI for visualization and business intelligence.

The model enables Power BI to analyze flight performance across multiple dimensions.

Recommended relationships include:

```text
Dim_Date[DateKey]
        |
        v
Fact_Flight[DateKey]
```

```text
Dim_Airline[CarrierKey]
        |
        v
Fact_Flight[CarrierKey]
```

```text
Dim_Route[RouteKey]
        |
        v
Fact_Flight[RouteKey]
```

```text
Dim_DelayCause[CauseKey]
        |
        v
Fact_Flight[CauseKey]
```

The dimension tables act as the descriptive side of the model, while the fact table contains the measurable flight events.

---

# 21. Power BI Analytical Use Cases

## Flight Delay Analysis

Possible KPIs:

* Total Flights
* Delayed Flights
* Delay Rate
* Average Arrival Delay
* Average Departure Delay
* Maximum Delay

## Airline Performance

Possible analysis:

* OTP by airline
* Delay rate by airline
* Average delay by airline
* Number of delayed flights by airline

## Route Performance

Possible analysis:

* Most delayed routes
* Most reliable routes
* Average delay by route
* Delay rate by route
* Distance vs delay

## Airport Congestion

Possible analysis:

* Flights by airport
* Delays by airport
* Average delay by airport
* Airport delay rate

## Time-Based Analysis

Possible analysis:

* Delay trend by year
* Delay trend by month
* Delay by day of week
* Delay by departure hour
* Delay by departure time bucket
* Weekend vs weekday delays

## Root Cause Analysis

Possible analysis:

* Delays by cause
* Total delay by cause
* Carrier delay contribution
* Weather delay contribution
* NAS delay contribution
* Security delay contribution
* Late aircraft contribution

---

# 22. Project Benefits

The solution provides a structured analytical foundation for understanding flight delays and operational performance.

The project can help stakeholders:

* Identify airlines with poor punctuality
* Identify problematic flight routes
* Detect high-delay airports
* Analyze airport traffic levels
* Identify peak delay periods
* Understand major operational delay causes
* Compare airline performance
* Compare route reliability
* Support scheduling decisions
* Support operational optimization
* Build scalable Power BI dashboards

---

# 23. End-to-End Pipeline

```text
Raw Flight Dataset
        |
        v
Azure Data Lake Storage Gen2
        |
        v
Bronze Layer
        |
        | Data Cleaning
        v
Silver Layer
        |
        | Feature Engineering
        v
Gold Layer
        |
        | Dimensional Modeling
        v
Star Schema
        |
        v
Power BI
        |
        v
Business Analytics
```

---

# 24. Project Structure

```text
Flight-Delay-Analysis/
│
├── README.md
│
├── Silver/
│   └── operated/
│
├── Gold/
│   └── flight_facts/
│
├── Modeling/
│   ├── dim_date/
│   ├── dim_airline/
│   ├── dim_airport/
│   ├── dim_route/
│   ├── dim_delaycause/
│   └── fact_flight/
│
├── Notebooks/
│   ├── Silver
│   ├── Gold_Transformation
│   └── Modeling
│
└── Presentation/
    └── Flight_Delay_Analysis_Presentation
```



