# Flight Delay Data Engineering & Analytics Platform

## 1. Project Overview

This project is an end-to-end data engineering solution for processing and preparing historical flight data for analytical use.

The project uses a Medallion Architecture to transform raw flight data into a clean, analysis-ready Gold layer and then organize the data into a Star Schema dimensional model.

The pipeline covers:

- Raw data ingestion
- Data cleaning and quality control
- Data standardization
- Flight classification
- Time-based feature engineering
- Delay classification
- Delay severity classification
- Route classification
- Delay cause analysis
- Dimensional modeling
- Fact and dimension table creation

The historical flight dataset covers the period from 2009 to 2018.

---

## 2. Project Objectives

The project is designed to support the following analytical objectives:

### 2.1 Flight Delay Analysis

Analyze historical flight data to identify delay patterns using:

- Departure delay
- Arrival delay
- Delay frequency
- Delay classification
- Delay severity
- Time-based delay features

### 2.2 Airline Performance Evaluation

Evaluate airline punctuality and operational performance using:

- Airline
- Arrival delay
- Delay status
- On-Time Performance (OTP)

### 2.3 Route Performance Analysis

Analyze route-level performance using:

- Origin airport
- Destination airport
- Route
- Distance
- Haul type
- Departure and arrival delays

### 2.4 Airport Congestion Analysis

Prepare airport-level data for analyzing:

- Flight volume
- Airport activity
- Delay frequency
- Arrival and departure delays

### 2.5 Time-Based Delay Pattern Analysis

Analyze delay behavior across:

- Year
- Month
- Day
- Day of week
- Departure hour
- Departure time bucket
- Weekend and weekday

### 2.6 Root Cause Analysis

Analyze the major categories of flight delays:

- Carrier
- Weather
- NAS
- Security
- Late Aircraft

---

# 3. Technology Stack

- Python
- PySpark
- Apache Spark
- Azure Synapse Analytics
- Azure Data Lake Storage Gen2
- CSV
- Parquet
- GitHub

---

# 4. Data Architecture

The project follows a Medallion Architecture.

```text
Raw Flight Data
       |
       v
+------------------+
| Bronze Layer     |
| Raw CSV Data     |
+------------------+
       |
       | Cleaning
       | Standardization
       v
+------------------+
| Silver Layer     |
| Operated Flights |
+------------------+
       |
       | Feature Engineering
       | Business Transformations
       v
+------------------+
| Gold Layer       |
| Analytical Data  |
+------------------+
       |
       | Dimensional Modeling
       v
+------------------+
| Star Schema      |
| Fact + Dimensions|
+------------------+
```

---

# 5. Bronze Layer

The Bronze layer contains the raw historical flight data stored in Azure Data Lake Storage Gen2.

The raw data is loaded from CSV files using PySpark.

```python
RAW_PATH = "abfss://bronze@flightdatalakegen2.dfs.core.windows.net/*.csv"

df_raw = spark.read \
    .option("header", "true") \
    .option("nullValue", "") \
    .option("mode", "PERMISSIVE") \
    .csv(RAW_PATH)
```

The Bronze layer is responsible for preserving the raw source data before applying analytical transformations.

---

# 6. Silver Layer

The Silver layer contains cleaned and standardized operated-flight data.

The main transformations performed in the Silver layer are:

- Removing unnecessary columns
- Removing duplicate records
- Separating cancelled and operated flights
- Cleaning invalid string values
- Casting numeric columns
- Handling missing values
- Removing records with missing essential identifiers
- Standardizing date and categorical fields
- Controlling extreme delay values
- Handling invalid air-time values
- Creating the route
- Creating the year partition column

---

## 6.1 Removing Unnecessary Columns

The source dataset contained an unnecessary column called `Unnamed: 27`.

It was removed because it does not provide useful analytical information.

```python
df = df_raw.drop("Unnamed: 27")
```

---

## 6.2 Removing Duplicate Flights

Duplicate records were removed using business-related flight attributes:

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

These columns help identify the same flight occurrence and prevent duplicated flights from affecting downstream analysis.

---

## 6.3 Separating Operated and Cancelled Flights

The dataset was divided into two groups:

```python
df_cancelled = df.filter(F.col("CANCELLED") == 1)

df_operated = df.filter(F.col("CANCELLED") == 0)
```

The operated flights are used as the main input for delay analysis because they contain actual departure and arrival information.

Cancelled flights are kept separately for potential cancellation-related analysis.

---

## 6.4 Cleaning Invalid Values

Invalid string values were replaced with null values.

```python
bad_values = [
    "undefined",
    "NA",
    "null",
    "NULL",
    "",
    " "
]

df_operated = df_operated.replace(bad_values, None)
```

The transformation improves data quality by preventing invalid textual values from being treated as meaningful data.

---

## 6.5 Numeric Columns

The following delay-related columns were converted to numeric values:

```python
numeric_cols = [
    "DEP_DELAY",
    "ARR_DELAY",
    "CARRIER_DELAY",
    "WEATHER_DELAY",
    "NAS_DELAY",
    "SECURITY_DELAY",
    "LATE_AIRCRAFT_DELAY"
]
```

They were cast to `double`:

```python
for c in numeric_cols:
    df = df.withColumn(
        c,
        F.col(c).cast("double")
    )
```

---

## 6.6 Missing Delay Values

Missing values in delay-related columns were replaced with zero:

```python
df = df.fillna({
    "DEP_DELAY": 0,
    "ARR_DELAY": 0,
    "CARRIER_DELAY": 0,
    "WEATHER_DELAY": 0,
    "NAS_DELAY": 0,
    "SECURITY_DELAY": 0,
    "LATE_AIRCRAFT_DELAY": 0
})
```

This allows delay calculations to be performed consistently.

---

## 6.7 Removing Records with Missing Identifiers

Records missing essential flight identifiers were removed:

```python
df_operated = df_operated.dropna(
    subset=[
        "FL_DATE",
        "OP_CARRIER",
        "ORIGIN",
        "DEST"
    ]
)
```

These fields are required for flight, airline, airport, and route analysis.

---

## 6.8 Standardizing Data Types and Formats

The flight date was converted to a date type.

Categorical fields were trimmed and converted to uppercase.

```python
df_operated = df_operated \
    .withColumn(
        "FL_DATE",
        F.to_date("FL_DATE", "yyyy-MM-dd")
    ) \
    .withColumn(
        "CANCELLED",
        F.col("CANCELLED").cast("int")
    ) \
    .withColumn(
        "DIVERTED",
        F.col("DIVERTED").cast("int")
    ) \
    .withColumn(
        "OP_CARRIER",
        F.upper(F.trim("OP_CARRIER"))
    ) \
    .withColumn(
        "ORIGIN",
        F.upper(F.trim("ORIGIN"))
    ) \
    .withColumn(
        "DEST",
        F.upper(F.trim("DEST"))
    )
```

This ensures consistency when the fields are later used for grouping, joins, and dimensional modeling.

---

## 6.9 Delay Data Quality Control

Extremely large delay values were capped at 1440 minutes:

```python
df_operated = df_operated \
    .withColumn(
        "ARR_DELAY",
        F.when(
            F.col("ARR_DELAY") > 1440,
            1440
        ).otherwise(F.col("ARR_DELAY"))
    ) \
    .withColumn(
        "DEP_DELAY",
        F.when(
            F.col("DEP_DELAY") > 1440,
            1440
        ).otherwise(F.col("DEP_DELAY"))
    )
```

This limits extreme values that may negatively affect analytical calculations.

---

## 6.10 Handling Invalid Air Time

Negative air-time values were considered invalid and converted to null:

```python
df_operated = df_operated.withColumn(
    "AIR_TIME",
    F.when(
        F.col("AIR_TIME") < 0,
        None
    ).otherwise(F.col("AIR_TIME"))
)
```

---

## 6.11 Creating the Route

A route was created by combining the origin and destination airports:

```python
df_operated = df_operated.withColumn(
    "ROUTE",
    F.concat_ws("-", "ORIGIN", "DEST")
)
```

Example:

```text
JFK-LAX
```

The `ROUTE` column enables route-level analysis.

---

## 6.12 Creating the Year Partition

The year was extracted from the flight date:

```python
df_operated = df_operated.withColumn(
    "YEAR",
    F.year("FL_DATE")
)
```

The Silver data was then stored using year partitioning:

```python
df_operated.write \
    .mode("overwrite") \
    .option("header", "true") \
    .partitionBy("YEAR") \
    .csv(
        SILVER_PATH + "operated/"
    )
```

Partitioning by year helps organize the historical dataset and supports efficient access to data by year.

---

# 7. Gold Layer

The Gold layer transforms the cleaned Silver data into an analysis-ready dataset.

Input:

```text
Silver/operated
```

Output:

```text
Gold/flight_facts
```

The Gold layer adds analytical features required for flight delay analysis.

---

# 8. Time-Based Features

The following columns are derived from `FL_DATE`:

```python
df = (df
    .withColumn("YEAR", F.year("FL_DATE"))
    .withColumn("MONTH", F.month("FL_DATE"))
    .withColumn("DAY", F.dayofmonth("FL_DATE"))
    .withColumn("DAY_OF_WEEK", F.dayofweek("FL_DATE"))
)
```

### YEAR

The year of the flight.

Used for yearly delay trends.

### MONTH

The month of the flight.

Used for identifying seasonal patterns.

### DAY

The day of the month.

Used for daily-level analysis.

### DAY_OF_WEEK

The day of the week.

Used to compare delay patterns across different days.

---

# 9. Departure Time Features

The scheduled departure time is transformed into an hour:

```python
df = df.withColumn(
    "DEP_HOUR",
    (F.col("CRS_DEP_TIME") / 100).cast(IntegerType())
)
```

A departure time bucket is then created:

```python
df = df.withColumn(
    "DEP_TIME_BUCKET",
    F.when(
        (F.col("DEP_HOUR") >= 6) &
        (F.col("DEP_HOUR") < 12),
        "Morning"
    )
    .when(
        (F.col("DEP_HOUR") >= 12) &
        (F.col("DEP_HOUR") < 18),
        "Afternoon"
    )
    .when(
        (F.col("DEP_HOUR") >= 18) &
        (F.col("DEP_HOUR") < 24),
        "Evening"
    )
    .otherwise("Night")
)
```

The resulting categories are:

```text
Morning
Afternoon
Evening
Night
```

These features support time-based delay analysis.

---

# 10. Delay Classification

## 10.1 IS_DELAYED

A flight is classified as delayed when arrival delay is at least 15 minutes.

```python
df = df.withColumn(
    "IS_DELAYED",
    F.when(
        F.col("ARR_DELAY") >= 15,
        1
    ).otherwise(0)
)
```

Meaning:

```text
1 = Delayed
0 = Not Delayed
```

This field can be used to calculate delay frequency and delay rate.

---

## 10.2 OTP_FLAG

The On-Time Performance flag is created using arrival delay:

```python
df = df.withColumn(
    "OTP_FLAG",
    F.when(
        F.col("ARR_DELAY") <= 15,
        1
    ).otherwise(0)
)
```

Meaning:

```text
1 = On Time
0 = Delayed
```

This feature supports airline and route punctuality analysis.

---

# 11. Delay Severity

The `DELAY_SEVERITY` column categorizes flights according to arrival delay:

```python
df = df.withColumn(
    "DELAY_SEVERITY",
    F.when(
        F.col("ARR_DELAY") < 0,
        "Early"
    )
    .when(
        F.col("ARR_DELAY") < 15,
        "On Time"
    )
    .when(
        F.col("ARR_DELAY") < 60,
        "Minor"
    )
    .when(
        F.col("ARR_DELAY") < 180,
        "Moderate"
    )
    .otherwise("Severe")
)
```

Categories:

```text
Early
On Time
Minor
Moderate
Severe
```

This provides a categorical representation of delay intensity.

---

# 12. Route and Distance Classification

## 12.1 ROUTE

The route is created using:

```python
ROUTE = ORIGIN + DEST
```

For example:

```text
JFK-LAX
```

The route is used for route-level performance analysis.

---

## 12.2 HAUL_TYPE

Flight distance is classified into three categories:

```python
df = df.withColumn(
    "HAUL_TYPE",
    F.when(
        F.col("DISTANCE") < 500,
        "Short"
    )
    .when(
        F.col("DISTANCE") < 1500,
        "Medium"
    )
    .otherwise("Long")
)
```

Categories:

```text
Short
Medium
Long
```

This enables comparison of delay behavior across different flight distances.

---

# 13. Delay Cause Analysis

The dataset contains five delay-cause components:

```text
CARRIER_DELAY
WEATHER_DELAY
NAS_DELAY
SECURITY_DELAY
LATE_AIRCRAFT_DELAY
```

These represent delay contributions associated with different operational causes.

---

## 13.1 MAX_DELAY

The largest delay component is calculated using:

```python
DELAY_COLS = [
    "CARRIER_DELAY",
    "WEATHER_DELAY",
    "NAS_DELAY",
    "SECURITY_DELAY",
    "LATE_AIRCRAFT_DELAY"
]

df = df.withColumn(
    "MAX_DELAY",
    F.greatest(
        *[
            F.coalesce(F.col(c), F.lit(0))
            for c in DELAY_COLS
        ]
    )
)
```

`MAX_DELAY` represents the largest delay contribution among the available delay-cause columns for a flight.

---

# 14. DELAY_CAUSE

The dominant delay cause is classified using the largest delay contribution.

```python
df = df.withColumn(
    "DELAY_CAUSE",
    F.when(
        F.col("IS_DELAYED") == 0,
        "No Delay"
    )
    .when(
        F.coalesce(F.col("CARRIER_DELAY"), F.lit(0))
        == F.col("MAX_DELAY"),
        "Carrier"
    )
    .when(
        F.coalesce(F.col("WEATHER_DELAY"), F.lit(0))
        == F.col("MAX_DELAY"),
        "Weather"
    )
    .when(
        F.coalesce(F.col("NAS_DELAY"), F.lit(0))
        == F.col("MAX_DELAY"),
        "NAS"
    )
    .when(
        F.coalesce(F.col("SECURITY_DELAY"), F.lit(0))
        == F.col("MAX_DELAY"),
        "Security"
    )
    .when(
        F.coalesce(F.col("LATE_AIRCRAFT_DELAY"), F.lit(0))
        == F.col("MAX_DELAY"),
        "Late Aircraft"
    )
    .otherwise("Unknown")
)
```

Possible values:

```text
No Delay
Carrier
Weather
NAS
Security
Late Aircraft
Unknown
```

This feature directly supports delay root-cause analysis.

---

# 15. TOTAL_CAUSE_DELAY

The five delay components are summed:

```python
df = df.withColumn(
    "TOTAL_CAUSE_DELAY",
    F.coalesce(F.col("CARRIER_DELAY"), F.lit(0)) +
    F.coalesce(F.col("WEATHER_DELAY"), F.lit(0)) +
    F.coalesce(F.col("NAS_DELAY"), F.lit(0)) +
    F.coalesce(F.col("SECURITY_DELAY"), F.lit(0)) +
    F.coalesce(F.col("LATE_AIRCRAFT_DELAY"), F.lit(0))
)
```

The resulting column represents the total delay attributed to the identified delay-cause components.

---

# 16. Gold Layer Storage

The Gold dataset is stored as Parquet and partitioned by year:

```python
df.write \
    .mode("overwrite") \
    .partitionBy("YEAR") \
    .parquet(GOLD_PATH)
```

Output:

```text
Gold/flight_facts/
```

---

# 17. Dimensional Modeling

After creating the analytical Gold layer, the data is transformed into a Star Schema.

The model consists of:

```text
                    Dim_Date
                       |
                       |
Dim_Airline ---- Fact_Flight ---- Dim_Route
                       |
                       |
                Dim_DelayCause
```

The airport information is also represented through the airport dimension.

The main model components are:

```text
Dim_Date
Dim_Airline
Dim_Airport
Dim_Route
Dim_DelayCause
Fact_Flight
```

---

# 18. Dim_Date

The date dimension starts from the distinct flight dates:

```python
dim_date = df.select("FL_DATE").distinct()
```

A surrogate key is generated:

```python
w = Window.orderBy("FL_DATE")

dim_date = dim_date.withColumn(
    "DateKey",
    F.row_number().over(w)
)
```

Additional date attributes are generated:

```python
dim_date = dim_date \
    .withColumn("Year", F.year("FL_DATE")) \
    .withColumn("Month", F.month("FL_DATE")) \
    .withColumn("Day", F.dayofmonth("FL_DATE")) \
    .withColumn("DayOfWeek", F.dayofweek("FL_DATE")) \
    .withColumn(
        "IsWeekend",
        F.when(
            F.dayofweek("FL_DATE").isin(1, 7),
            1
        ).otherwise(0)
    )
```

The dimension contains:

```text
DateKey
FL_DATE
Year
Month
Day
DayOfWeek
IsWeekend
```

---

# 19. Dim_Airline

The airline dimension is created from unique operating carriers:

```python
dim_airline = df.select(
    "OP_CARRIER"
).distinct()
```

A surrogate key is generated:

```python
w = Window.orderBy("OP_CARRIER")

dim_airline = dim_airline.withColumn(
    "CarrierKey",
    F.row_number().over(w)
)
```

The dimension contains:

```text
CarrierKey
OP_CARRIER
```

---

# 20. Dim_Airport

The airport dimension combines airports appearing as either origin or destination.

```python
airports = df.select(
    F.col("ORIGIN").alias("AIRPORT")
).union(
    df.select(
        F.col("DEST").alias("AIRPORT")
    )
).distinct()
```

A surrogate key is generated:

```python
w = Window.orderBy("AIRPORT")

dim_airport = airports.withColumn(
    "AirportKey",
    F.row_number().over(w)
)
```

The dimension contains:

```text
AirportKey
AIRPORT
```

---

# 21. Dim_Route

The route dimension is created from unique route combinations:

```python
dim_route = df.select(
    "ROUTE",
    "DISTANCE",
    "HAUL_TYPE"
).distinct()
```

A surrogate key is generated:

```python
w = Window.orderBy("ROUTE")

dim_route = dim_route.withColumn(
    "RouteKey",
    F.row_number().over(w)
)
```

The dimension contains:

```text
RouteKey
ROUTE
DISTANCE
HAUL_TYPE
```

---

# 22. Dim_DelayCause

The delay-cause dimension contains the distinct delay-cause categories:

```python
dim_cause = df.select(
    "DELAY_CAUSE"
).distinct()
```

A surrogate key is generated:

```python
w = Window.orderBy("DELAY_CAUSE")

dim_cause = dim_cause.withColumn(
    "CauseKey",
    F.row_number().over(w)
)
```

The dimension contains:

```text
CauseKey
DELAY_CAUSE
```

---

# 23. Fact_Flight

The fact table contains flight-level analytical measurements and foreign keys to the dimensions.

The selected source attributes are:

```python
fact = df.select(
    "FL_DATE",
    "OP_CARRIER",
    "ORIGIN",
    "DEST",
    "ROUTE",
    "DELAY_CAUSE",
    "DEP_DELAY",
    "ARR_DELAY",
    "DISTANCE",
    "AIR_TIME",
    "IS_DELAYED",
    "OTP_FLAG",
    "TOTAL_CAUSE_DELAY"
)
```

The dimension keys are selected before joining:

```python
dim_date = dim_date.select(
    "FL_DATE",
    "DateKey"
)

dim_airline = dim_airline.select(
    "OP_CARRIER",
    "CarrierKey"
)

dim_airport = dim_airport.select(
    "AIRPORT",
    "AirportKey"
)

dim_route = dim_route.select(
    "ROUTE",
    "RouteKey"
)

dim_cause = dim_cause.select(
    "DELAY_CAUSE",
    "CauseKey"
)
```

The dimension keys are then joined to the fact data:

```python
fact = fact \
    .join(
        dim_date,
        "FL_DATE",
        "left"
    ) \
    .join(
        dim_airline,
        "OP_CARRIER",
        "left"
    ) \
    .join(
        dim_route,
        "ROUTE",
        "left"
    ) \
    .join(
        dim_cause,
        "DELAY_CAUSE",
        "left"
    )
```

A left join is used so that all flight records from the fact dataset are preserved even if a corresponding dimension key cannot be found.

The final fact table contains the dimension keys and analytical measures:

```python
fact_final = fact.select(
    "DateKey",
    "CarrierKey",
    "RouteKey",
    "CauseKey",
    "ORIGIN",
    "DEST",
    "DEP_DELAY",
    "ARR_DELAY",
    "DISTANCE",
    "AIR_TIME",
    "IS_DELAYED",
    "OTP_FLAG",
    "TOTAL_CAUSE_DELAY"
)
```

The final fact table is stored as Parquet:

```python
fact_final.write \
    .mode("overwrite") \
    .parquet(
        "abfss://gold@flightdatalakegen2.dfs.core.windows.net/fact_flight/"
    )
```

---

# 24. Final Data Model

The final dimensional model contains the following tables:

```text
                         Dim_Date
                            |
                            |
                            v
Dim_Airline ----------> Fact_Flight <---------- Dim_Route
                            |
                            |
                            v
                     Dim_DelayCause
```

The airport dimension is maintained separately for airport-level analytical modeling.

### Dimensions

```text
Dim_Date
- DateKey
- FL_DATE
- Year
- Month
- Day
- DayOfWeek
- IsWeekend
```

```text
Dim_Airline
- CarrierKey
- OP_CARRIER
```

```text
Dim_Airport
- AirportKey
- AIRPORT
```

```text
Dim_Route
- RouteKey
- ROUTE
- DISTANCE
- HAUL_TYPE
```

```text
Dim_DelayCause
- CauseKey
- DELAY_CAUSE
```

### Fact

```text
Fact_Flight
- DateKey
- CarrierKey
- RouteKey
- CauseKey
- ORIGIN
- DEST
- DEP_DELAY
- ARR_DELAY
- DISTANCE
- AIR_TIME
- IS_DELAYED
- OTP_FLAG
- TOTAL_CAUSE_DELAY
```

---

# 25. Project Outcomes

The implemented solution provides:

- Raw flight data ingestion
- Data cleaning and quality control
- Duplicate removal
- Operated and cancelled flight separation
- Data standardization
- Time-based feature engineering
- Departure time classification
- Delay classification
- Delay severity classification
- Route creation
- Flight distance classification
- Delay cause classification
- Total cause delay calculation
- Analytical Gold layer
- Date dimension
- Airline dimension
- Airport dimension
- Route dimension
- Delay-cause dimension
- Flight fact table
- Star Schema dimensional model

---

# 26. Repository Structure

```text
Flight-Delay-Data-Engineering/
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

---

# 27. Conclusion

This project implements an end-to-end flight data engineering pipeline using Azure Data Lake Storage Gen2, Azure Synapse Analytics, and PySpark.

The pipeline transforms raw historical flight records through Bronze, Silver, and Gold layers, applies data quality and analytical transformations, and finally organizes the data into a Star Schema consisting of fact and dimension tables.

The resulting analytical data model provides a structured foundation for studying flight delays, airline performance, route performance, airport activity, time-based delay patterns, and delay causes.
