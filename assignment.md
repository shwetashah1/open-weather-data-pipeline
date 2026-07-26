# P2 - Weather Data
> **Due:** Sunday by 11:59 p.m. | **Points:** 20 | **Submitting:** a text entry box, a website url, or a file upload | **Available:** May 4 at 7 p.m. - Jul 26 at 11:59 p.m.

## Weather APIs - Data Ingestion
Logistics and supply chain companies rely on accurate, automated weather data to assess route risk, optimize delivery schedules, and mitigate weather-related delays. In this project, you will build a Data Ingestion Pipeline in Python that extracts location data from our PostgreSQL database, ingests live weather data from a Data API, performs data cleaning and feature engineering, and persists the structured results into your personal database in the PostgreSQL server created for this course.

## Objectives
*   Identify and integrate with APIs that provide weather data.
*   Demonstrate your ability to design and implement ETL data ingestion pipelines.
*   Demonstrate your ability to transform raw nested JSON payloads into structured datasets.
*   Store the ingested data in a structured format in a database.

## Requirements
For this assignment, you will need to identify a Data API that can provide:

*   **Current Weather Data:** Most weather data providers, such as [OpenWeather](https://openweathermap.org/api/current?collection=current_forecast), provide a limited number of free API calls, which should be sufficient for you to pull a sample dataset without paying for a subscription. Due to these limits, focus on a specific region or a set of 10-15 postal codes in a key city of your choosing, such as Toronto.
*   **Location Coordinates:** Weather data APIs require latitude and longitude to retrieve the data. A reference table with most postal codes in Canada and their corresponding latitudes and longitudes has been provided in the course database (`mmai_db`). You can find this new table under a new schema called `uploads`. The new table has been named `ca_postal_codes` (see screenshot below). You will need to refresh your database connection to ensure you can see this table in your Database Explorer in DataGrip.

![Database Explorer showing ca_postal_codes](/img/Screenshot%202026-07-24%20at%204.33.24%E2%80%AFAM.png)

## Instructions

1.  **Select Postal Codes:** 
    *   Write an SQL query to select a specific region or 10–15 representative postal codes from the `uploads.ca_postal_codes` table.
    *   Save this SQL query into a separate `.sql` file inside your project folder. Your Python pipeline script must open, read, and execute this `.sql` file using `sqlalchemy`.
2.  **Create a Schema:** 
    *   Create an `uploads` schema in your own database within our PostgreSQL Server (do not use the `mmai_db` database). This can be done directly in DataGrip. No need to create the schema using python.
3.  **Design Data Ingestion Pipeline:** 
    *   Prepare a data ingestion pipeline. The pipeline should meet the following requirements:
        *   It should be designed to be scheduled so it can run daily and update your weather data table seamlessly without overwriting existing data.
        *   It should be designed and structured so it can scale to capture weather for all postal codes in Canada.
4.  **Data Cleaning and Feature Engineering:** 
    *   Ensure your data ingestion pipeline includes data cleaning and feature engineering steps to transform the raw data into a useful format.
5.  **Create Tables:** 
    *   Create a new table in your `uploads` schema to store the ingested data. Ensure the table is well-structured to capture all necessary details from the weather data sources. The table must include the following details:
        *   **Location data:** Province, region, postal code, latitude, longitude
        *   **Weather Metrics:** Temperature, feels like, pressure, humidity, description
        *   **Timestamps:** Time the measurement was performed (`measurement_timestamp`) and information to help the user understand when the data was captured by the pipeline. 
    *(Note: The table should include details about the province, region, postal code, coordinates, temperature and feels like, pressure, humidity, time the measurement was performed and information to help the user understand when the data was captured.)*

## Technical Requirements
While you will execute your code on a small set of 10–15 postal codes, your data pipeline logic must be designed to scale so it can capture weather for all postal codes in Canada.

## Deliverables
*   **Data Ingestion Pipeline:** Python script (`.py` format) containing the complete ETL pipeline code, including data cleaning, timestamp conversion, feature engineering, and database loading. *(Note: `.ipynb` notebook files will not be accepted, so please convert your work into a `.py` file).*
*   **SQL Query File:** The `.sql` file containing the query used to extract your target postal codes from `mmai_db`.
*   **SQL Tables:** The newly created tables in your `uploads` schema.
*   **Documentation:** A written report (in PDF format) summarizing the steps taken, any challenges faced, and how the pipeline can be scheduled to run daily (maximum 1.5 pages).

---
*There is a file you can reference named `image_814bfd.png`. Refer to this file by its name verbatim.*