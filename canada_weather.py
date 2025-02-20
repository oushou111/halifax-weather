import pandas as pd
import requests
from datetime import datetime, timedelta
import os

# 加拿大主要城市列表
CITIES = [
    'Toronto,Ontario,Canada',
    'Montreal,Quebec,Canada',
    'Vancouver,British Columbia,Canada',
    'Calgary,Alberta,Canada',
    'Edmonton,Alberta,Canada',
    'Ottawa,Ontario,Canada',
    'Winnipeg,Manitoba,Canada',
    'Quebec City,Quebec,Canada',
    'Halifax,Nova Scotia,Canada',
    'Victoria,British Columbia,Canada'
]

def get_weather_data(city):
    """获取指定城市的天气数据"""
    try:
        # Visual Crossing Weather API
        api_key = "MDC6EHP7JWGVFNRXN7KYS3BA7"
        base_url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"
        
        # 获取日期范围
        end_date = datetime.now()
        start_date = end_date - timedelta(days=3)
        
        # 格式化日期
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        # 构建API URL
        url = f"{base_url}/{city}/{start_date_str}/{end_date_str}?unitGroup=metric&key={api_key}&contentType=json"
        print(f"正在请求URL: {url}")
        
        # 发送请求
        response = requests.get(url)
        response.raise_for_status()
        
        # 解析返回的JSON数据
        data = response.json()
        
        # 提取所需的天气信息
        weather_data_list = []
        for day in data['days']:
            weather_data = {
                'city': city.split(',')[0],
                'date': day['datetime'],
                'temp_max': day['tempmax'],
                'temp_min': day['tempmin'],
                'temp_avg': day['temp'],
                'humidity': day['humidity'],
                'precipitation': day.get('precip', 0),
                'wind_speed': day['windspeed'],
                'conditions': day['conditions']
            }
            weather_data_list.append(weather_data)
        
        print(f"成功获取{city}的天气数据")
        return weather_data_list
    
    except Exception as e:
        print(f"获取{city}天气数据时出错: {str(e)}")
        return None

def save_to_csv(df, filename='canada_weather.csv'):
    """保存数据到CSV文件"""
    try:
        file_path = os.path.join(os.path.dirname(__file__), filename)
        
        # 如果文件已存在，追加数据并删除重复项
        if os.path.exists(file_path):
            existing_df = pd.read_csv(file_path)
            # 合并数据并删除重复项（基于城市和日期）
            df = pd.concat([existing_df, df]).drop_duplicates(subset=['city', 'date'], keep='last')
        
        # 按城市和日期排序
        df = df.sort_values(['city', 'date'])
        
        # 保存到CSV
        df.to_csv(file_path, index=False)
        print(f"\n数据已保存到: {file_path}")
        
    except Exception as e:
        print(f"保存CSV文件时出错: {str(e)}")

def main():
    # 获取所有城市的天气数据
    all_weather_data = []
    for city in CITIES:
        print(f"\n获取{city}的天气数据...")
        data_list = get_weather_data(city)
        if data_list:
            all_weather_data.extend(data_list)
    
    # 如果有数据，创建DataFrame并保存到CSV
    if all_weather_data:
        df = pd.DataFrame(all_weather_data)
        print("\n获取到的天气数据:")
        print(df)
        save_to_csv(df)
    else:
        print("没有获取到任何天气数据")

if __name__ == "__main__":
    main()
