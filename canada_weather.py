import os
import requests
from datetime import datetime
import pandas as pd
from google.cloud import bigquery
from google.api_core import exceptions

# 加拿大主要城市列表
CITIES = [
    'Toronto',
    'Montreal',
    'Vancouver',
    'Calgary',
    'Halifax'
]

# BigQuery配置
PROJECT_ID = 'canada-weather-451503'  # Google Cloud项目ID
DATASET_ID = 'weather_data'     # BigQuery数据集名称
TABLE_ID = 'canada_weather'     # BigQuery表名称

def get_weather_data(city):
    """获取指定城市的天气数据"""
    try:
        # WeatherAPI.com API
        api_key = os.getenv('WEATHER_API_KEY', '6e7f288838454892a5e215301252002')
        base_url = "http://api.weatherapi.com/v1/current.json"
        
        # 构建API URL - 使用quote()进行URL编码
        location = f"{city},Canada"
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

def ensure_bigquery_resources():
    """确保BigQuery数据集和表存在"""
    try:
        # 设置认证信息
        credentials = service_account.Credentials.from_service_account_file(
            'service-account-key.json',
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        
        # 初始化BigQuery客户端
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        
        # 创建数据集（如果不存在）
        dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
        try:
            client.get_dataset(dataset_ref)
            print(f"数据集 {dataset_ref} 已存在")
        except exceptions.NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"  # 设置数据集位置
            client.create_dataset(dataset)
            print(f"已创建数据集 {dataset_ref}")
        
        # 创建表（如果不存在）
        table_ref = f"{dataset_ref}.{TABLE_ID}"
        try:
            client.get_table(table_ref)
            print(f"表 {table_ref} 已存在")
        except exceptions.NotFound:
            # 定义表结构
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
            print(f"已创建表 {table_ref}")
            
        return True
    except Exception as e:
        print(f"创建BigQuery资源时出错: {str(e)}")
        return False

def save_to_bigquery(df):
    """保存数据到BigQuery"""
    try:
        # 设置认证信息
        credentials = service_account.Credentials.from_service_account_file(
            'service-account-key.json',
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

        # 将数据写入BigQuery
        df.to_gbq(
            destination_table=f"{DATASET_ID}.{TABLE_ID}",
            project_id=PROJECT_ID,
            credentials=credentials,
            if_exists='append'  # 追加新数据
        )
        
        print(f"\n数据已成功保存到BigQuery表: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
        print(f"保存了 {len(df)} 条记录")
        return True
    except Exception as e:
        print(f"保存到BigQuery时出错: {str(e)}")
        return False

def main():
    """主函数"""
    try:
        # 确保BigQuery资源存在
        if not ensure_bigquery_resources():
            print("无法创建必要的BigQuery资源，程序退出")
            return
            
        # 存储所有城市的天气数据
        all_weather_data = []
        
        # 获取每个城市的天气数据
        for city in CITIES:
            weather_data = get_weather_data(city)
            if weather_data:
                all_weather_data.append(weather_data)
        
        # 如果没有获取到任何数据，退出
        if not all_weather_data:
            print("未能获取任何天气数据")
            return
        
        # 创建DataFrame
        df = pd.DataFrame(all_weather_data)
        
        # 保存到BigQuery
        save_to_bigquery(df)
        
    except Exception as e:
        print(f"运行程序时出错: {str(e)}")

if __name__ == "__main__":
    main()
