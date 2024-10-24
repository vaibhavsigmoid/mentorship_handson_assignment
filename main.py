# main.py
from fetch_data import process_data
from upload_s3 import upload_to_s3

import os
# from credentials import aws_access_key_id, aws_secret_access_key
from dotenv import load_dotenv

load_dotenv("credentials.env")
aws_access_key_id = os.getenv("aws_access_key_id")
aws_secret_access_key = os.getenv("aws_secret_access_key")

# List of 50 countries
countries = [
    "USA", "India", "Brazil", "Russia", "France", "UK", "Italy", "Germany", "Spain", "Argentina",
    "Colombia", "Mexico", "Poland", "Iran", "South Africa", "Ukraine", "Peru", "Indonesia",
    "Netherlands", "Turkey", "Iraq", "Philippines", "Pakistan", "Belgium", "Israel",
    "Sweden", "Chile", "Bangladesh", "Japan", "Romania", "Canada", "Morocco", "Switzerland",
    "Austria", "Portugal", "Nepal", "Czechia", "Hungary", "Jordan", "Serbia", "Greece",
    "Malaysia", "Venezuela", "Finland", "Norway", "Ireland", "Australia", "New Zealand",
    "Thailand", "South Korea", "Denmark"
]

# S3 configuration
s3_bucket = "diseaseassignment"
s3_file_path = "covid_data/covid_cases.csv"

def main():
    # Fetch and process data
    df_with_total = process_data(countries)

    # Convert Spark DataFrame to Pandas for uploading to S3
    pandas_df = df_with_total.toPandas()

    # Upload the processed data to S3
    upload_to_s3(pandas_df, s3_bucket, s3_file_path, aws_access_key_id, aws_secret_access_key)

if __name__ == "__main__":
    main()
