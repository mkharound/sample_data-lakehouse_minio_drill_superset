# https://api.energidataservice.dk/dataset/ElectricityProdex5MinRealtime?offset=0&start=2022-01-01T00:00&sort=Minutes5UTC%20DESC&timezone=dk6

import csv
from os.path import exists
def storeCSV(filename, dictList):
    """
    Store a list of dicts as CSV file
    filename: Name of file to store data in 
    """
    fileexists = exists(filename)
    with open(filename, 'a+', newline='') as f:  
        w = csv.DictWriter(f, dictList[0].keys()) 
        if not fileexists:
            w.writeheader()
        w.writerows(dictList)


import requests


url='https://api.energidataservice.dk/dataset/ElectricityProdex5MinRealtime'
params = {
    'limit': 1,
    'offset': 0,
    'start': '2022-01-01T00:00',
    'sort': 'Minutes5UTC DESC',
    'timezone': 'dk6'
}

print(params)

response = requests.get(url, params=params )

result = response.json()

print(result.keys())
print(result['total'])
print(result['limit'])

size = result['total']
#limit = result['limit']
limit = 10000

recordstotal = 0
for offset in range(0, size, limit):
    
    url='https://api.energidataservice.dk/dataset/ElectricityProdex5MinRealtime'
    params = {
        'limit': limit,
        'offset': offset,
        'start': '2022-01-01T00:00',
        'sort': 'Minutes5UTC DESC',
        'timezone': 'dk6'
    }
    
    response = requests.get(url, params=params )
    result = response.json()

    storeCSV('powprodexp.csv', result['records'])

    recordstotal += len(result["records"])
    print(f'offset: {offset}, records: { len(result["records"]) }, acc records: {recordstotal}')
