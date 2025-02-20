import pandas as pd
import requests
from datetime import datetime
import time
from urllib.parse import quote
import os

# 加拿大主要城市列表
CITIES = [
    'Toronto',
    'Montreal',
    'Vancouver',
    'Calgary',
    'Halifax'
]

def get_weather_data(city):
    """获取指定城市的天气数据"""
    try:
        # WeatherAPI.com API
        api_key = os.getenv('WEATHER_API_KEY', '6e7f288838454892a5e215301252002')  # 使用环境变量或默认值
        base_url = "http://api.weatherapi.com/v1/current.json"
        
        # 构建API URL - 使用quote()进行URL编码
        location = quote(f"{city},Canada")
        url = f"{base_url}?key={api_key}&q={location}&aqi=no"
        print(f"\n正在请求{city}的天气数据...")
        
        # 发送请求
        response = requests.get(url)
        
        # 检查响应状态
        if response.status_code != 200:
            print(f"API请求失败: {response.status_code}")
            print(f"错误信息: {response.text}")
            return None
            
        # 解析返回的JSON数据
        data = response.json()
        
        # 提取所需的天气数据
        weather_data = {
            'city': city,
            'date': datetime.now().strftime('%Y-%m-%d'),
            'temperature': data['current']['temp_c'],
            'humidity': data['current']['humidity'],
            'wind_speed': data['current']['wind_kph'],
            'description': data['current']['condition']['text'],
            'pressure': data['current']['pressure_mb']
        }
        
        print(f"成功获取{city}的天气数据")
        return weather_data
        
    except Exception as e:
        print(f"获取{city}天气数据时出错: {str(e)}")
        return None

def save_to_csv(df, filename='canada_weather.csv'):
    """保存数据到CSV文件"""
    try:
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"\n数据已成功保存到{filename}")
        print("\n保存的数据预览:")
        print(df)
        return True
    except Exception as e:
        print(f"保存CSV文件时出错: {str(e)}")
        return False

def main():
    """主函数"""
    try:
        # 存储所有城市的天气数据
        all_weather_data = []
        
        # 获取每个城市的天气数据
        for city in CITIES:
            weather_data = get_weather_data(city)
            if weather_data:
                all_weather_data.append(weather_data)
            time.sleep(1)  # 添加延迟以避免超过API速率限制
        
        # 如果没有获取到任何数据，退出
        if not all_weather_data:
            print("未能获取任何天气数据")
            return
        
        # 创建DataFrame并保存到CSV
        df = pd.DataFrame(all_weather_data)
        save_to_csv(df)
        
    except Exception as e:
        print(f"运行程序时出错: {str(e)}")

if __name__ == "__main__":
    main()
