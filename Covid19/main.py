import os
import boto3
import pandas as pd
from io import StringIO

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, max, when, udf
from pyspark.sql.types import StringType, Row

load_dotenv("../credentials.env")
aws_access_key_id = os.getenv("aws_access_key_id")
aws_secret_access_key = os.getenv("aws_secret_access_key")

bucket_name = 'covid19sigmoid'
file_path = './covid_19_data.csv'
s3_key = 'raw/covid_19_data_raw.csv'

continent_map = {
        # Asia
        "China": "Asia", "India": "Asia", "Japan": "Asia", "South Korea": "Asia", "Indonesia": "Asia",
        "Hong Kong": "Asia",
        "Malaysia": "Asia", "Singapore": "Asia", "Thailand": "Asia", "Vietnam": "Asia", "Iran": "Asia",
        "Israel": "Asia",

        # Africa
        "Nigeria": "Africa", "South Africa": "Africa", "Egypt": "Africa", "Kenya": "Africa", "Morocco": "Africa",

        # Europe
        "France": "Europe", "Germany": "Europe", "United Kingdom": "Europe", "Italy": "Europe", "Spain": "Europe",
        "Netherlands": "Europe", "Belgium": "Europe", "Sweden": "Europe", "Russia": "Europe", "Poland": "Europe",

        # North America
        "United States": "North America", "Canada": "North America", "Mexico": "North America", "USA": "North America",

        # South America
        "Brazil": "South America", "Argentina": "South America", "Colombia": "South America", "Chile": "South America",
        "Peru": "South America",

        # Oceania
        "Australia": "Oceania", "New Zealand": "Oceania"
    }


def upload_csv_to_s3(s3_client, csv_content, bucket_name, s3_key):
    s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_content)
    print(f"File uploaded to {s3_key} in S3 bucket successfully")

def get_continent(country):
    return continent_map.get(country, "Unknown")

def dataframe_to_csv_content(df):
    csv_buffer = StringIO()
    df.toPandas().to_csv(csv_buffer, index=True)
    return csv_buffer.getvalue()

if __name__ == "__main__":
    # try:
    #     upload_csv_to_s3(file_path,bucket_name,s3_key)
    #     print("file uploaded successsfully")
    # except:
    #     raise("error!!!!!")

    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    s3_client = session.client('s3')

    # Download the file into memory
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
    file_content = response['Body'].read().decode('utf-8')

    # Converting file content into a Pandas DataFrame
    csv_data = StringIO(file_content)
    pdf = pd.read_csv(csv_data)

    # Spark session
    spark = SparkSession.builder.appName("S3ToSparkDF").getOrCreate()

    # Convert the Pandas DataFrame to a Spark DataFrame
    df = spark.createDataFrame(pdf)

    ## CLEANING
    print(df.count())
    df_cleaned = df.dropna(subset=["Confirmed", "Deaths", "Recovered"])
    df_cleaned = df_cleaned.withColumn("Confirmed", col("Confirmed").cast("double")) \
        .withColumn("Deaths", col("Deaths").cast("double")) \
        .withColumn("Recovered", col("Recovered").cast("double"))
    df_cleaned.show(5)
    print(df_cleaned.count())

    df_deduped = df_cleaned.dropDuplicates(["Country/Region","ObservationDate"])
    df_deduped.show(1000)

    ## FILTERING

    country_totals = df_cleaned.groupBy("Country/Region") \
        .agg(sum("Confirmed").alias("TotalConfirmed"))
    country_totals_filter = country_totals.filter(col("TotalConfirmed") > 100000)
    # country_totals_filter.show(10)
    filtered_df = df_cleaned.join(country_totals_filter, "Country/Region", "inner")
    filtered_df = filtered_df.select("SNo","ObservationDate","Country/Region","Last Update", "Confirmed", "Deaths", "Recovered").orderBy("SNo")
    filtered_df.show()

    ## GROUP BY and AGGREGATION

    aggregated_df = filtered_df.groupBy("Country/Region").agg(
        sum("Confirmed").alias("TotalConfirmed"),
        sum("Deaths").alias("TotalDeaths"),
        sum("Recovered").alias("TotalRecovered")
    )

    aggregated_df = aggregated_df.withColumn(
        "RecoveryRate",
        (col("TotalRecovered") / col("TotalConfirmed")) * 100  # Convert to percentage
    )
    aggregated_df = aggregated_df.withColumn(
        "MortalityRate" ,
        (col("TotalDeaths")/col("TotalConfirmed")) * 100
    )
    aggregated_df.show(100)
    aggregated_df = aggregated_df.withColumn(
        "MortalityCategory",
        when(col("MortalityRate") > 5, "High")\
        .when((col("MortalityRate") >=1)  & (col("MortalityRate") <= 5), "Moderate")\
        .otherwise("Low")
    )
    aggregated_df.show(100)


    ## REDUCE

    continent_udf = udf(get_continent, StringType())

    aggregated_df_new = aggregated_df.withColumn("Continent", continent_udf(col("Country/Region")))
    continent_rdd = aggregated_df_new.select("Continent","TotalConfirmed", "TotalDeaths").rdd
    continent_rdd_map = continent_rdd.map(lambda row: (row[0],(row[1],row[2])))

    aggregated_rdd = continent_rdd_map.reduceByKey(lambda x,y : (x[0]+y[0], x[1]+y[1]))

    agg_df = aggregated_rdd.map(lambda x: Row(Continent=x[0], TotalConfirmed=x[1][0]/10000, TotalDeaths=x[1][1]/10000))
    agg_df = spark.createDataFrame(agg_df)

    agg_df.show(5)


    ## SQL (tempView)
    # Query 1: Top 5 countries with the highest number of confirmed cases

    aggregated_df.createOrReplaceTempView("covid_data")

    top_5_confirmed = spark.sql("""
        Select `Country/Region`, (TotalConfirmed/100000) as `TotalConfirmed(Lks)`
        From covid_data
        order by TotalConfirmed desc
        LIMIT 5
    """)

    print("Top 5 countries with the highest number of confirmed cases:")
    top_5_confirmed.show()

    # Query 2: Countries with the highest death-to-confirmed ratio (mortality rate)
    highest_mortality_rate = spark.sql("""
        SELECT `Country/Region`, (TotalDeaths/100000) as `TotalDeaths(Lks)`, 
                (TotalConfirmed/100000) as `TotalConfirmed(Lks)`,
               (TotalDeaths * 100 / TotalConfirmed) AS `MortalityRate(%)`
        FROM covid_data
        WHERE TotalConfirmed > 0
        ORDER BY MortalityRate DESC
        LIMIT 5
    """)
    print("Countries with the highest death-to-confirmed ratio (mortality rate):")
    highest_mortality_rate.show()

    # Query 3: Top 3 countries with the most active cases on the most recent date
    df_cleaned.createOrReplaceTempView("covid_data1")

    # Query to get the most recent date in the dataset
    most_recent_date = spark.sql("""
        SELECT MAX(ObservationDate) AS MostRecentDate
        FROM covid_data1
    """).collect()[0]['MostRecentDate']

    # Query to calculate active cases and find the top 3 countries with the most active cases
    top_3_active_cases = spark.sql(f"""
        SELECT `Country/Region`, ((SUM(Confirmed - Deaths - Recovered))/100000) AS ActiveCases_Lks
        FROM covid_data1
        WHERE ObservationDate <= '{most_recent_date}'
        GROUP BY `Country/Region`
        ORDER BY ActiveCases_Lks DESC
        LIMIT 3
    """)

    top_3_active_cases.show()

    upload_csv_to_s3(s3_client, dataframe_to_csv_content(aggregated_df), bucket_name, 'processed/aggregated_df.csv')
    upload_csv_to_s3(s3_client, dataframe_to_csv_content(agg_df), bucket_name, 'processed/agg_df.csv')
    upload_csv_to_s3(s3_client, dataframe_to_csv_content(top_5_confirmed), bucket_name, 'processed/top_5_confirmed.csv')
    upload_csv_to_s3(s3_client, dataframe_to_csv_content(highest_mortality_rate), bucket_name, 'processed/highest_mortality_rate.csv')
    upload_csv_to_s3(s3_client, dataframe_to_csv_content(top_3_active_cases), bucket_name,'processed/top_3_active_cases.csv')



