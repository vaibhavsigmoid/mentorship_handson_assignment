import requests
import pandas as pd

# List of 50 countries (you can modify this list with the countries of your choice)
countries = [
    "USA", "India", "Brazil", "Russia", "France", "UK", "Italy", "Germany", "Spain", "Argentina",
    "Colombia", "Mexico", "Poland", "Iran", "South Africa", "Ukraine", "Peru", "Indonesia",
    "Netherlands", "Turkey", "Iraq", "Philippines", "Pakistan", "Belgium", "Israel",
    "Sweden", "Chile", "Bangladesh", "Japan", "Romania", "Canada", "Morocco", "Switzerland",
    "Austria", "Portugal", "Nepal", "Czechia", "Hungary", "Jordan", "Serbia", "Greece",
    "Malaysia", "Venezuela", "Finland", "Norway", "Ireland", "Australia", "New Zealand",
    "Thailand", "South Korea", "Denmark"
]


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


# Fetch data for all countries and organize into a DataFrame
def get_data_for_countries(countries, days=100):
    all_data = []
    dates = None

    for country in countries:
        country_data = fetch_country_data(country, days=days)

        # Get sorted dates and the corresponding case numbers
        if country_data:
            sorted_dates = sorted(country_data.keys())  # Sort by date to ensure order
            cases = [country_data[date] for date in sorted_dates]

            # Save the first set of dates (once only)
            if dates is None:
                dates = sorted_dates

            # Add data to the list for DataFrame creation
            all_data.append([country] + cases)

    # Create DataFrame: First column is 'Country', followed by dates as columns
    columns = ['Country'] + dates
    df = pd.DataFrame(all_data, columns=columns)

    return df


# Fetch and store the data for the specified countries
df = get_data_for_countries(countries, days=30)

# Display the first few rows of the dataframe
print(df.head())

# Save the DataFrame to a CSV file if needed
df.to_csv("covid_daily_cases_50_countries.csv", index=False)
