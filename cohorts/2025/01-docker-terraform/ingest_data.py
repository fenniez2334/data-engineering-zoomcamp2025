#!/usr/bin/env python
# coding: utf-8

import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    table_name2 = params.table_name2
    url = params.url
    url2 = params.url2
    
    zipped_name = 'green_taxi_trips.csv.gz'
    csv_name = 'green_taxi_trips.csv'
    csv_name2 = 'zone.csv'
    # download csv
    os.system(f"wget {url} -O {zipped_name}")
    os.system(f"gunzip {zipped_name}")
    os.system(f"wget {url2} -O {csv_name2}")

    # Write to CSV 
    df = pd.read_csv(csv_name)
    df2 = pd.read_csv(csv_name2)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # engine.connect()

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(0).to_sql(con=engine,name=table_name, if_exists='replace')

    df.to_sql(con=engine,name=table_name, if_exists='append')

    df2.to_sql(name=table_name2,con=engine, if_exists="replace", index=False)

    # get_ipython().run_line_magic('time', "df.to_sql(con=engine,name='yellow_taxi_data', if_exists='append')")



    while True:
        try:
            t_start = time()

            df = next(df_iter)
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

            df.to_sql(con=engine,name=table_name, if_exists='append')

            t_end = time()

            print(f'inserted another chunk..., took {t_end - t_start} seconds.')
        except StopIteration:
            print('Finished ingesting data into the postgres database')
            break
        

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    # user, password, host, port, database name, tablename, url of the csv
    parser.add_argument('--user', help='user name for postgres')          
    parser.add_argument('--password', help='password for postgres') 
    parser.add_argument('--host', help='host for postgres') 
    parser.add_argument('--port', help='port for postgres') 
    parser.add_argument('--db', help='database name for postgres') 
    parser.add_argument('--table_name', help='name of the table where we will write the results to') 
    parser.add_argument('--table_name2', help='name of the table where we will write the results to') 
    parser.add_argument('--url', help='url of the csv file') 
    parser.add_argument('--url2', help='url of thessecond csv file') 

    args = parser.parse_args()
    main(args)


