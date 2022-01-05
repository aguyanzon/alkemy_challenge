import csv
from datetime import datetime
import logging
import os

import pandas as pd
import requests


URLS = {
    'museos' : 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museos.csv',
    'cines' : 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/392ce1a8-ef11-4776-b280-6f1c7fae16ae/download/cine.csv',
    'bibliotecas' : 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/01c6c048-dbeb-44e0-8efa-6944f73715d7/download/biblioteca_popular.csv'
}

TODAY = datetime.today()


def make_dir(path):
    """Create a new folder if it doesn't exist and join it with the working directory"""
    try:
        directory = os.path.join(os.getcwd(), path)
        if not os.path.exists(directory):
            os.makedirs(directory)
            logging.info(f"Directory: {directory} created successfully")
        else:
            logging.info(f"Directory: {directory} already exists")
            
    except BaseException as error:
        logging.critical(f"Unexpected {error= }, {type(error)= }")
        

def download_data_files():
    """Download the files through request. It performs a decoding for each of them, 
    then converts them into a dataframe and finally hosts them as csv files in a new folder 
    created in conjunction with the make_dir function.
    """
    for file_name, url in URLS.items():
        with requests.Session() as s:
            
            date = TODAY.strftime("%Y-%B")
            folder = os.path.join(file_name, date)
            make_dir(folder)
            
            download = s.get(url)
            logging.info("Downloading {} file".format(url))

            # Evaluate differents encodings
            decoded_content = download.content.decode(download.apparent_encoding)

            csv_reader = csv.reader(decoded_content.splitlines(), delimiter=',')
            df = pd.DataFrame(csv_reader)
            df.to_csv(
                f"{folder}/{file_name}-{TODAY.strftime('%d-%m-%Y')}.csv",
                index=False
            )
