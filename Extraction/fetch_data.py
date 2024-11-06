# fetch_data.py
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, LongType


# Function to fetch data for a particular country
def fetch_country_data(country, days=30):
    url = f"https://disease.sh/v3/covid-19/historical/{country}?lastdays={days}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        timeline = data.get('timeline', {}).get('cases', {})
        return timeline
    else:
        print(f"Failed to fetch data for {country}")
        return {}


# Fetch data for all countries
def get_data_for_countries(countries, days=100):
    all_data = []
    dates = None

    for country in countries:
        country_data = fetch_country_data(country, days=days)
        if country_data:
            sorted_dates = sorted(country_data.keys())
            cases = [country_data[date] for date in sorted_dates]

            if dates is None:
                dates = sorted_dates

            all_data.append([country] + cases)

    return all_data, dates


# Function to process the data using Spark and return a DataFrame
def process_data(countries, days=100):
    spark = SparkSession.builder.appName("CovidData").getOrCreate()

    # Fetch data for the last 100 days
    data, dates = get_data_for_countries(countries, days)

    schema_fields = [StructField("Country", StringType(), True)] + \
                    [StructField(date, LongType(), True) for date in dates]
    schema = StructType(schema_fields)

    df = spark.createDataFrame(data, schema)

    # Calculate total cases
    date_columns = dates
    df_with_total = df.withColumn("Total Cases", sum([col(date) for date in date_columns]))

    return df_with_total
