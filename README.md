# COVID-19 Data Analysis
This project involves analyzing a COVID-19 dataset from Johns Hopkins University using PySpark. The dataset contains information on confirmed cases, deaths, recoveries, and active cases for various countries over time. The analysis includes data cleaning, filtering, aggregation, and SQL-based insights.

## Dataset
The dataset used in this analysis has the following columns:

Country/Region: The country or region where the data was recorded.
Date: The date of the record.
Confirmed: The total confirmed COVID-19 cases.
Deaths: The total recorded deaths due to COVID-19.
Recovered: The total number of recoveries.
Active: The current active COVID-19 cases.
## Analysis Steps
### 1. Null Handling and Redundancy Removal
Loaded the dataset and removed rows where critical values like Confirmed, Deaths, or Recovered are missing.
Removed redundant entries for the same country on the same date.
### 2. Data Filtering
Filtered out countries with fewer than a specified number of total confirmed cases (e.g., less than 10,000 cases).
Focused on specific continents by filtering (e.g., Europe).
### 3. Mapping and Adding New Columns
Created a new column for the mortality rate (calculated as Deaths / Confirmed).
Classified mortality rates into categories:
High (> 5%)
Moderate (1-5%)
Low (< 1%)
### 4. GroupBy and Aggregation
Grouped data by Country/Region to aggregate:
Total confirmed cases
Total deaths
Total recovered cases
Average recovery rate across countries
### 5. ReduceByKey Operations
Used reduceByKey to compute total confirmed cases and deaths by continent.
### 6. TempView and Spark SQL Analysis
Created a temporary view of the data and used Spark SQL to analyze:
Top 5 Countries: Countries with the highest number of confirmed cases.
High Mortality Countries: Countries with the highest death-to-confirmed ratio.
Top 3 Active Cases: Countries with the most active cases on the most recent date.
### Requirements
Apache Spark
PySpark
How to Run
Ensure that Apache Spark and PySpark are set up on your environment.
Load the dataset into a DataFrame and follow the steps as described in the analysis.py script.

### Screenshot
![img](https://github.com/user-attachments/assets/f8e88925-80a1-4452-b55c-d2d27f58805e)

![img_1](https://github.com/user-attachments/assets/79f82123-7313-4c50-b07b-dc708f0e4372)

![img_2](https://github.com/user-attachments/assets/5fdd0731-f6e1-4f15-b787-6ad43bc116f9)

![img_3](https://github.com/user-attachments/assets/269179c5-cd7a-4adb-99d7-1255064eace4)

![img_4](https://github.com/user-attachments/assets/57e9a265-7e1b-4d58-ac74-db3a130e9fec)

![img_5](https://github.com/user-attachments/assets/83aa1993-bce3-4258-b002-4e5dc04ec366)

![img_6](https://github.com/user-attachments/assets/dfc9f959-7632-4851-ba21-a6e98801f386)

![img_7](https://github.com/user-attachments/assets/0d1e24d4-2c53-4f05-8d2a-83b6410f63d9)









