<<<<<<< HEAD
# Halifax Weather Data Collector

This script collects weather data for Halifax for the past 3 days and stores it in a CSV file. The data is updated daily at midnight.

## Setup

1. Sign up for a free API key at [Visual Crossing Weather API](https://www.visualcrossing.com/weather-api)
2. Replace `YOUR_API_KEY` in `halifax_weather.py` with your actual API key
3. Install requirements:
   ```
   pip install -r requirements.txt
   ```

## Running the Script

Simply run:
```
python halifax_weather.py
```

The script will:
- Immediately collect weather data for the past 3 days
- Create/update `halifax_weather.csv` on your desktop
- Continue running and update the data daily at midnight

## Output Data

The CSV file contains the following information for each day:
- Date
- Maximum temperature
- Minimum temperature
- Average temperature
- Humidity
- Precipitation
- Wind speed
- Weather conditions
=======
# halifax-weather
Daily Halifax weather data collection using GitHub Actions
>>>>>>> df933971f819b6165478964a7587152583b3be46
