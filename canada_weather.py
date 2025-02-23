import os
import requests
from datetime import datetime
import pandas as pd
from google.cloud import bigquery
from google.api_core import exceptions
from google.oauth2 import service_account

# List of major Canadian cities
CITIES = [
    'Toronto',
    'Montreal',
    'Vancouver',
    'Calgary',
    'Halifax'
]

# BigQuery settings
PROJECT_ID = 'canada-weather-451503'  # Google Cloud project ID
DATASET_ID = 'weather_data'     # BigQuery dataset name
TABLE_ID = 'canada_weather'     # BigQuery table name

def get_weather_data(city):
    """
    Fetch weather data for a given city using WeatherAPI
    Args:
        city (str): Name of the city
    Returns:
        dict: Weather data including temperature, humidity, etc.
    """
    try:
        # WeatherAPI.com API
        api_key = os.getenv('WEATHER_API_KEY', '6e7f288838454892a5e215301252002')
        base_url = "http://api.weatherapi.com/v1/current.json"
        
        # Build API URL
        location = f"{city},Canada"
        url = f"{base_url}?key={api_key}&q={location}&aqi=no"
        print(f"\nFetching weather data for {city}...")
        
        # Make API request
        response = requests.get(url)
        
        # Check response status
        if response.status_code != 200:
            print(f"API request failed: {response.status_code}")
            print(f"Error message: {response.text}")
            return None
            
        # Parse returned JSON data
        data = response.json()
        
        # Extract relevant weather information
        weather_data = {
            'city': city,
            'date': datetime.now().strftime('%Y-%m-%d'),
            'temperature': data['current']['temp_c'],
            'humidity': data['current']['humidity'],
            'wind_speed': data['current']['wind_kph'],
            'description': data['current']['condition']['text'],
            'pressure': data['current']['pressure_mb']
        }
        
        print(f"Successfully fetched data for {city}")
        return weather_data
        
    except Exception as e:
        print(f"Error fetching data for {city}: {str(e)}")
        return None

def ensure_bigquery_resources():
    """
    Ensure BigQuery dataset and table exist
    Creates them if they don't exist
    """
    try:
        # Set up authentication
        credentials = service_account.Credentials.from_service_account_file(
            'service-account-key.json',
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        
        # Initialize BigQuery client
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        
        # Check and create dataset if needed
        dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
        try:
            client.get_dataset(dataset_ref)
            print(f"Dataset {dataset_ref} exists")
        except exceptions.NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"  # Set dataset location
            client.create_dataset(dataset)
            print(f"Created dataset {dataset_ref}")
        
        # Check and create table if needed
        table_ref = f"{dataset_ref}.{TABLE_ID}"
        try:
            client.get_table(table_ref)
            print(f"Table {table_ref} exists")
        except exceptions.NotFound:
            # Define table schema
            schema = [
                bigquery.SchemaField("city", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
                bigquery.SchemaField("temperature", "FLOAT", mode="REQUIRED"),
                bigquery.SchemaField("humidity", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("wind_speed", "FLOAT", mode="REQUIRED"),
                bigquery.SchemaField("description", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("pressure", "FLOAT", mode="REQUIRED"),
            ]
            
            table = bigquery.Table(table_ref, schema=schema)
            client.create_table(table)
            print(f"Created table {table_ref}")
        
        return True
    except Exception as e:
        print(f"Error ensuring BigQuery resources: {str(e)}")
        return False

def save_to_bigquery(df):
    """
    Save weather data to BigQuery
    Args:
        df (pandas.DataFrame): DataFrame containing weather data
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Set up authentication
        credentials = service_account.Credentials.from_service_account_file(
            'service-account-key.json',
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

        # Write data to BigQuery
        df.to_gbq(
            destination_table=f"{DATASET_ID}.{TABLE_ID}",
            project_id=PROJECT_ID,
            credentials=credentials,
            if_exists='append'  # Append new data
        )
        
        print(f"\nSuccessfully saved data to BigQuery table: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
        print(f"Saved {len(df)} records")
        return True
    except Exception as e:
        print(f"Error saving to BigQuery: {str(e)}")
        return False

def main():
    """Main function to fetch and save weather data"""
    try:
        # Ensure BigQuery resources exist
        if not ensure_bigquery_resources():
            print("Unable to create necessary BigQuery resources. Exiting...")
            return
            
        # Initialize list to store weather data
        all_weather_data = []
        
        # Fetch weather data for each city
        for city in CITIES:
            weather_data = get_weather_data(city)
            if weather_data:
                all_weather_data.append(weather_data)
        
        # Exit if no data was fetched
        if not all_weather_data:
            print("No weather data was collected. Exiting...")
            return
        
        # Create DataFrame
        df = pd.DataFrame(all_weather_data)
        
        # Save to BigQuery
        save_to_bigquery(df)
        
    except Exception as e:
        print(f"Error in main function: {str(e)}")

if __name__ == "__main__":
    main()
