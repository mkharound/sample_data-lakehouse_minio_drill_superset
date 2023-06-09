from typing import List
import os                                   # contact to Operative System

import airflow
from airflow.decorators import dag, task

from decouple import config                 # secret configs
import requests                             #  for making HTTP requests
import time
from datetime import datetime               # for date and time manipulation
from datetime import timedelta        # time, time time

from operator import itemgetter             # for sorting dicts
#from geopy.distance import distance         # calculate distance between geopos

import csv                                  # writing csv files

import json
import pendulum

## inspired by https://stackoverflow.com/questions/48393065/run-apache-airflow-dag-without-apache-airflow
if __name__ != "__main__":
    from airflow.decorators import dag, task
else:
    mock_decorator = lambda f=None,**d: f if f else lambda x:x
    dag = mock_decorator
    task = mock_decorator


def pull_data(service: str, data_dir: str, data_name: str, data_timedate: datetime, page_size: int, params: dict) -> List[str]:
    """
        Hjælpefuntion til at lave api requests

    """

    nFeatures = 0
    page_index = 0

    fNames = []

    while True:
        if not 'limit' in params.keys():
            params['limit'] = page_size
        params['offset'] = page_index * page_size

        r = requests.get(URL+service, params=params)
        if r.status_code != requests.codes.ok: # response 200 etc
            print(r)
            #print(r.headers)
            print(r.text)
            print(params)
            raise Exception('http not 200 ok', r.text)
            break
        else:
            rjson = r.json() # Extract JSON object
            print(f"Total number of records: {len(rjson['records'])}")
            # nFeatures += len(rjson['features'])
            #print(rjson)
            print(params)

        if len(rjson['records']) > 0:
            page_index += 1
            time_stamp = data_timedate.isoformat(timespec='seconds').replace(':', '.')
            fName = f'{data_dir}/{data_name}_{time_stamp}_#{page_index}.json'
            with open(fName, "w+") as f:
                f.write(r.text)
            fNames.append(fName)
        else:
            break
        time.sleep(1)
    return fNames




def setups():
    """Getting config variable from system.
    `.env` file og `setting.ini` (or maybe even environment vars?).
    Some of these are secret like api keys password etc.
    These shold NOT be in souce-code, where it could accidentially be shared to others.
    Therefore the file `.env` id NOT commited to git but added to `.gitignore`.
    There is an example `.env.example`, withou real sensible values."""
    global URL, data_dir, page_size
    URL = 'https://api.energidataservice.dk/'
    data_dir = './dags/EnergiDataService/data'
    page_size = 1000


default_task_args = {
    'retries' : 10,
    'retry_delay' : timedelta(minutes=1),
    'retry_exponential_backoff' : True,
}

@task
def extract_ElectricityProdex(**kwargs):
    """
    Produceret og import/export af el fra forskellige typer kilder
    hvert 5. minut

    https://www.energidataservice.dk/tso-electricity/ElectricityProdex5MinRealtime#metadata-info
    https://www.energidataservice.dk/guides/api-guides

    """
    global URL, data_dir, page_size
    service = 'dataset/ElectricityProdex5MinRealtime'

    params = {}
    #params['limit'] = 4
    page_size = 2

    ts = datetime.fromisoformat(kwargs['ts'])

    params['start'] = (ts - timedelta(minutes=9)).replace(tzinfo=None).isoformat(timespec='minutes')
    params['end']   = ts.replace(tzinfo=None).isoformat(timespec='minutes')

    print(params['start'], params['end'])

    return pull_data(service, data_dir, 'ElectricityProdex', ts, page_size, params)
    #https://api.energidataservice.dk/dataset/ElectricityProdex5MinRealtime?offset=0&start=2022-12-26T00:00&end=2022-12-27T00:00&sort=Minutes5UTC%20DESC&timezone=dk

@task
def write_to_bucket(eProdex_jsons, minio=None):
    # tweet_list, batchDatetime_str, batchId = data
    # batchDatetime = datetime.strptime(batchDatetime_str, "%Y-%m-%d %H:%M:%S")

    import pandas as pd
    from minio import Minio
    from io import BytesIO

    # MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
    MINIO_BUCKET_NAME = 'prodex'
    MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
    MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")

    MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
    MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')

    # MINIO_ACCESS_KEY = os.getenv('MINIO_ROOT_USER')
    # MINIO_SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD')

    client = Minio("minio:9000", access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

    # Make MINIO_BUCKET_NAME if not exist.
    found = client.bucket_exists(MINIO_BUCKET_NAME)
    if not found:
        client.make_bucket(MINIO_BUCKET_NAME)
    else:
        print(f"Bucket '{MINIO_BUCKET_NAME}' already exists!")

    for prodex_json in eProdex_jsons:

        # df = pd.DataFrame(tweet_list)
        # file_data = df.to_parquet(index=False)
        df = pd.read_json(prodex_json)
        file_data = df.to_parquet(index=False)

        # Put parquet data in the bucket
        filename = (
            # f"tweets/{batchDatetime.strftime('%Y/%m/%d')}/elon_tweets_{batchDatetime.strftime('%H%M%S')}_{batchId}.parquet"
            f"/{prodex_json}.parquet"
        )
        client.put_object(
            MINIO_BUCKET_NAME, filename, data=BytesIO(file_data), length=len(file_data), content_type="application/csv"
        )


@dag(
    dag_id='electrical_power_gross',
    schedule=timedelta(minutes=5),
    start_date=pendulum.datetime(2013, 6, 1, 0, 0, 0, tz="Europe/Copenhagen"),
    catchup=True,
    max_active_tasks=5,
    max_active_runs=5,
    tags=['experimental', 'energy', 'rest api'],
    default_args=default_task_args,)
def electrical_power_gross():
    print("Doing energy_data")
    setups()
    if __name__ != "__main__": # as in "normal" operation as DAG stated in Airflow
        eProdex_jsons = extract_ElectricityProdex()
    else: # more or less test mode
        eProdex_jsons = extract_ElectricityProdex(ts=datetime.now().isoformat())
    write_to_bucket(eProdex_jsons)

@task
def extract_ElectricityProdex_back(**kwargs):
    """
    Produceret og import/export af el fra forskellige typer kilder
    hent historisk data

    https://www.energidataservice.dk/tso-electricity/ElectricityProdex5MinRealtime#metadata-info
    https://www.energidataservice.dk/guides/api-guides

    """
    global URL, data_dir, page_size
    service = 'dataset/ElectricityProdex5MinRealtime'

    params = {}
    #params['limit'] = 4
    #page_size = 500

    #print('kwargs:', kwargs)
    for k, v in kwargs.items():
        print(k, '=', v)

    ts = datetime.fromisoformat(kwargs['ts'])

    params['start'] = kwargs['data_interval_start'].replace(tzinfo=None).isoformat(timespec='minutes')
    params['end']   = kwargs['data_interval_end'].replace(tzinfo=None).isoformat(timespec='minutes')

    #print(params['start'], params['end'])

    return pull_data(service, data_dir, 'ElectricityProdex_back', ts, page_size, params)
    #return 'dummy'
    #https://api.energidataservice.dk/dataset/ElectricityProdex5MinRealtime?offset=0&start=2022-12-26T00:00&end=2022-12-27T00:00&sort=Minutes5UTC%20DESC&timezone=dk


@dag(
    dag_id='electrical_power_gross_back',
    schedule='@monthly',
    end_date=pendulum.datetime(2023, 6, 1, 0, 0, 0, tz="Europe/Copenhagen"),
    start_date=pendulum.datetime(2014, 12, 31, 23, 0, 0, tz="Europe/Copenhagen"),
    catchup=True,
    max_active_tasks=5,
    max_active_runs=5,
    tags=['experimental', 'energy', 'rest api'],
    default_args=default_task_args,)
def electrical_power_gross_back():
    print("Doing energy_data")
    setups()
    if __name__ != "__main__": # as in "normal" operation as DAG stated in Airflow
        eProdex_jsons = extract_ElectricityProdex_back()
    else: # more or less test mode
        args = {
            'ts': datetime.now().isoformat(),
            'data_interval_end' : datetime.fromisoformat("2021-01-31T23:00:00+00:00"),
            'data_interval_start' : datetime.fromisoformat("2020-12-31T23:00:00+00:00"),
        }
        eProdex_jsons = extract_ElectricityProdex_back(**args)
    write_to_bucket(eProdex_jsons)


electrical_power_gross()
electrical_power_gross_back()