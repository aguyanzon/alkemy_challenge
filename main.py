import csv
from datetime import datetime
import json
import locale
import logging
import os

from decouple import config
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import requests


locale.setlocale(locale.LC_ALL, 'es_ES')

URLS = {
    'museos' : 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museos.csv',
    'cines' : 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/392ce1a8-ef11-4776-b280-6f1c7fae16ae/download/cine.csv',
    'bibliotecas' : 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/01c6c048-dbeb-44e0-8efa-6944f73715d7/download/biblioteca_popular.csv'
}


logger = logging.getLogger(__name__)
c_handler = logging.StreamHandler()
c_handler.setLevel(config("LOG_INFO", cast=int))
c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
c_handler.setFormatter(c_format)
logger.addHandler(c_handler)


def make_dir(path):
    """
    Create a new folder if it doesn't exist and join it with the working directory 
    """
    try:
        directory = os.path.join(os.getcwd(), path)
        if not os.path.exists(directory):
            os.makedirs(directory)
            logger.info("Directory: ", directory, "created successfully")
        else:
            logger.info("Directory: ", directory, "already exists")
            
    except BaseException as error:
        logger.critical(f"Unexpected {error= }, {type(error)= }")
        

def download_data_files():
    """
    Download the files through request. It performs a decoding for each of them, 
    then converts them into a dataframe and finally hosts them as csv files in a new folder 
    created in conjunction with the make_dir function.
    """
    for name_file, url in URLS.items():
        with requests.Session() as s:
            
            date = datetime.now().strftime("%Y-%B")
            folder = "{}\{}".format(name_file,date)
            make_dir(folder)
            
            download = s.get(url)
            logger.info("Downloading {} file".format(url))

            # Evaluate differents encodings
            if download.apparent_encoding == 'ISO-8859-1':
                decoded_content = download.content.decode('ISO-8859-1')
                
            elif download.apparent_encoding == 'utf-8':
                decoded_content = download.content.decode('utf-8')
                
            else:
                decoded_content = download.content.decode('latin-1')

            csv_reader = csv.reader(decoded_content.splitlines(), delimiter=',')
            df = pd.DataFrame(csv_reader)
            df.to_csv("{}/{}-{}.csv".format(folder, name_file, datetime.today().strftime('%d-%m-%Y')), index=False)



if __name__ == "__main__":
    download_data_files()