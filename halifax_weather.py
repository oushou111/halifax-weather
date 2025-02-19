import requests
import pandas as pd
from datetime import datetime, timedelta
import os

def get_weather_data():
    # Visual Crossing Weather API endpoint
    base_url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"
    
    # Get API key from environment variable
    api_key = os.getenv('WEATHER_API_KEY', "MDC6EHP7JWGVFNRXN7KYS3BA7")  # 如果环境变量不存在，使用默认key
    
    # Location
    location = "Halifax,NS,Canada"
    
    # Calculate dates
    end_date = datetime.now()
    start_date = end_date - timedelta(days=3)
    
    # Format dates
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    # Build URL
    url = f"{base_url}/{location}/{start_date_str}/{end_date_str}?unitGroup=metric&key={api_key}&include=days"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        # Extract daily weather data
        weather_data = []
        for day in data['days']:
            weather_data.append({
                'date': day['datetime'],
                'temp_max': day['tempmax'],
                'temp_min': day['tempmin'],
                'temp_avg': day['temp'],
                'humidity': day['humidity'],
                'precipitation': day['precip'],
                'wind_speed': day['windspeed'],
                'conditions': day['conditions']
            })
        
        # Convert to DataFrame
        df = pd.DataFrame(weather_data)
        
        # Save to CSV
        output_file = os.path.join(os.path.dirname(__file__), 'halifax_weather.csv')
        
        # If file exists, append without header; if not, create with header
        if os.path.exists(output_file):
            existing_df = pd.read_csv(output_file)
            # Remove duplicates based on date if any
            combined_df = pd.concat([existing_df, df]).drop_duplicates(subset=['date'], keep='last')
            combined_df.to_csv(output_file, index=False)
        else:
            df.to_csv(output_file, index=False)
            
        print(f"Weather data updated successfully at {datetime.now()}")
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data: {e}")

if __name__ == "__main__":
    get_weather_data()
