import pandas as pd
import argparse
from time import time
from sqlalchemy import create_engine
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    csv_path = params.csv_path

    # 创建数据库连接
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    try:
        df_iter = pd.read_csv(csv_path, iterator=True, chunksize=100000)
        for df in df_iter:
            # 数据类型转换
            # df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            # df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

            # 将数据插入数据库
            df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            print('Inserted another chunk...')
    except Exception as e:
        print(f'postgresql://{user}:{password}@{host}:{port}/{db}')
        print(f"Error during data processing: {e}")
    finally:
        engine.dispose()  # 确保连接关闭

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--csv_path', help='relative path of the csv file')

    args = parser.parse_args()
    print(args)
    main(args)



"""
python ingest_data.py `
--user=root `
--password=root `
--host=localhost `
--port=5432 `
--db=ny_taxi `
--table_name=taxi_zone `
--csv_path=./taxi_zone_lookup.csv
"""