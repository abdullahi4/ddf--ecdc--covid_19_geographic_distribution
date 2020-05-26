import requests
import json
import os.path as osp

url = 'https://opendata.ecdc.europa.eu/covid19/casedistribution/csv'

curdir = osp.dirname(osp.abspath(__file__))
source_dir = osp.join(curdir,'..','source')
source_path = osp.join(source_dir, 'COVID-19-geographic-disbtribution-worldwide.csv')

def get_latest():

    print('Updating source...')

    r = requests.get(url)
    with open(source_path, '+w', newline='') as f:
        f.write(r.text)

if (__name__ == '__main__'):
    get_latest()