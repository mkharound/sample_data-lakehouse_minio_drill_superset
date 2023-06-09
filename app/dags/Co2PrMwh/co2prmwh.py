import os
import requests
import time
from datetime import datetime
from datetime import timedelta
from typing import List
from airflow.decorators import dag, task
import pendulum


default_task_args = {
    'retries' : 10,
    'retry_delay' : timedelta(minutes=1),
    'retry_exponential_backoff' : True,
}

def setup():

    global URL, data_dir, page_size

    URL = 'https://api.energidataservice.dk/'

    data_dir = './dags/Co2PrMwh/data'
    #data_dir = os.getcwd()+'/data'

    page_size = 576

@task
def write_to_bucket(jsons, minio=None):

    import pandas as pd
    from minio import Minio
    from io import BytesIO

    MINIO_BUCKET_NAME = 'co2'
    MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
    MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")

    #MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
    #MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')

    MINIO_ACCESS_KEY = os.getenv('MINIO_ROOT_USER')
    MINIO_SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD')

    client = Minio("minio:9000", access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

    found = client.bucket_exists(MINIO_BUCKET_NAME)

    if not found:
        client.make_bucket(MINIO_BUCKET_NAME)

    else:
        print(f"Bucket '{MINIO_BUCKET_NAME}' already exists!")

    for json in jsons:

        df = pd.read_json(json)
        file_data = df.to_parquet(index=False)

        filename = (
            f"/{json}.parquet"
        )
        client.put_object(
            MINIO_BUCKET_NAME, filename, data=BytesIO(file_data), length=len(file_data), content_type="application/csv"
        )

def pull_data(service: str, data_dir: str, data_name: str, data_timedate: datetime, page_size: int, params: dict)\
        -> List[str]:

    page_index = 0

    fNames = []

    while True:

        if not 'limit' in params.keys():
            params['limit'] = page_size
        params['offset'] = page_index * page_size

        r = requests.get(URL+service, params=params)

        if r.status_code != requests.codes.ok:
            raise Exception('error 200', r.text)

        else:
            rjson = r.json()
            print(f"Total number of records: {len(rjson['records'])}")
            print(params)

        if len(rjson['records']) > 0:

            page_index += 1

            time_stamp = datetime.fromisoformat(rjson['records'][1]['Minutes5DK']).strftime('%Y-%m-%d')

            fName = f'{data_dir}/{data_name}_{time_stamp}.json'

            with open(fName, "w+") as f:
                f.write(r.text)
                f.close()

            fNames.append(fName)

        else:
            break

        time.sleep(3)
    return fNames

@task
def extract_co2(**kwargs):

    service = 'dataset/CO2Emis'

    params = {}

    page_size = 2

    ts = datetime.fromisoformat(kwargs['ts'])

    params['start'] = (ts - timedelta(minutes=9)).replace(tzinfo=None).isoformat(timespec='minutes')
    params['end']   = ts.replace(tzinfo=None).isoformat(timespec='minutes')

    print(params['start'], params['end'])

    return pull_data(service, data_dir, 'co2', ts, page_size, params)

@dag(
    dag_id='co2_gross',
    schedule=timedelta(minutes=5),
    start_date=pendulum.datetime(2017, 1, 1, 0, 0, 0, tz="Europe/Copenhagen"),
    catchup=True,
    max_active_tasks=5,
    max_active_runs=5,
    tags=['experimental', 'energy', 'rest api'],
    default_args=default_task_args,)
def co2_gross():

    setup()

    if __name__ != "__main__":
        co2_jsons = extract_co2()

    else:
        co2_jsons = extract_co2(ts=datetime.now().isoformat())

    #write_to_bucket(co2_jsons)

#@task
def extract_co2_old(**kwargs):

    service = 'dataset/CO2Emis'

    params = {}

    ts = datetime.fromisoformat(kwargs['ts'])

    params['start'] = kwargs['data_interval_start'].replace(tzinfo=None).isoformat(timespec='minutes')

    params['end']   = kwargs['data_interval_end'].replace(tzinfo=None).isoformat(timespec='minutes')

    return pull_data(service, data_dir, 'co2_old', ts, page_size, params)

@dag(
    dag_id='co2_old',
    schedule='@monthly',
    end_date=pendulum.datetime(2023, 6, 1, 0, 0, 0, tz="Europe/Copenhagen"),
    start_date=pendulum.datetime(2017, 1, 1, 0, 0, 0, tz="Europe/Copenhagen"),
    catchup=True,
    max_active_tasks=5,
    max_active_runs=5,
    tags=['experimental', 'energy', 'rest api'],
    default_args=default_task_args,)
def co2_old():

    setup()

    if __name__ != "__main__":
        co2_jsons = extract_co2_old()

    else:
        args = {
            'ts': datetime.now().isoformat(),
            'data_interval_end' : datetime.fromisoformat("2021-01-31T23:00:00+00:00"),
            'data_interval_start' : datetime.fromisoformat("2020-12-31T23:00:00+00:00"),
        }
        co2_jsons = extract_co2_old(**args)

    print(co2_jsons)

    #write_to_bucket(co2_jsons)


#co2_gross()
co2_old()