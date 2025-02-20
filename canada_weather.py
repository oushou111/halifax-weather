import pandas as pd
import requests
from datetime import datetime, timedelta
import os
from supabase import create_client
import time

# Supabase configuration
SUPABASE_URL = os.getenv('SUPABASE_URL', 'https://mrmlhepxjdsxcvvyyolb.supabase.co')
SUPABASE_KEY = os.getenv('SUPABASE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im1ybWxoZXB4amRzeGN2dnl5b2xiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDAwMjY1NTksImV4cCI6MjA1NTYwMjU1OX0.VdNbtsms1rsHerXmJT42Y6wD9V6QQtyPIoKpJaFHucs')

# 加拿大主要城市列表
CITIES = [
    'Toronto,Ontario,Canada',
    'Montreal,Quebec,Canada',
    'Vancouver,British Columbia,Canada',
    'Calgary,Alberta,Canada',
    'Halifax,Nova Scotia,Canada'
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
        
        # 如果遇到429错误，等待一分钟后重试
        if response.status_code == 429:
            print(f"API请求过于频繁，等待60秒后重试...")
            time.sleep(60)
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
        
        # 在请求之间添加延迟
        time.sleep(2)
        
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

def save_to_supabase(df):
    """保存数据到Supabase"""
    try:
        # 创建Supabase客户端
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        # 将DataFrame转换为字典列表
        records = df.to_dict('records')
        
        # 转换日期格式
        for record in records:
            record['date'] = pd.to_datetime(record['date']).strftime('%Y-%m-%d')
        
        # 使用upsert来更新或插入数据
        result = supabase.table('weather_data').upsert(
            records,
            on_conflict=['city', 'date']  # 基于城市和日期的唯一约束
        ).execute()
        
        print(f"\n数据已成功保存到Supabase")
        return True
        
    except Exception as e:
        print(f"保存到Supabase时出错: {str(e)}")
        return False

def main():
    # 获取所有城市的天气数据
    all_weather_data = []
    for city in CITIES:
        print(f"\n获取{city}的天气数据...")
        data_list = get_weather_data(city)
        if data_list:
            all_weather_data.extend(data_list)
    
    # 如果有数据，创建DataFrame并保存
    if all_weather_data:
        df = pd.DataFrame(all_weather_data)
        print("\n获取到的天气数据:")
        print(df)
        
        # 保存到CSV
        save_to_csv(df)
        
        # 保存到Supabase
        save_to_supabase(df)
    else:
        print("没有获取到任何天气数据")

if __name__ == "__main__":
    main()
