from datetime import datetime
import csv
import json
import locale
import logging
import os
import time

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
    """Create a new folder if it doesn't exist and join it with the working directory"""
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
    """Download the files through request. It performs a decoding for each of them, 
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
            df.to_csv("{}/{}-{}.csv".format(folder, name_file, 
                                            datetime.today().strftime('%d-%m-%Y')),
                                            index=False)


def read_file_csv(name):
    """Read csv files and convert them to dataframe"""
    folder = datetime.now().strftime("%Y-%B")
    file = datetime.today().strftime('%d-%m-%Y')
    data = pd.read_csv('./{}/{}/{}-{}.csv'.format(name, folder, name, file), header=1)
        
    return data


def georef_reverse_geocode(data, fields, params=None, prefix='gr_', step_size=1000):
    """Function extracted from the API of the argentine geographic data normalization
    service that allows to normalize and encode the names of territorial units in Argentina.
    """
    if not params:
        params = {}
        
    geocoded = pd.DataFrame()

    # Split the input DataFrame into 'step_size' parts of length
    for i in range(0, len(data), step_size):
        end = min(len(data), i + step_size)
        queries = []

        for j in range(i, end):
            # Each individual 'query' is created (equivalent to making a GET request),
            # and is added to a query list.
            query = {
                'aplanar': True,
                **params
            }

            for key, value in fields.items():
                query[key] = data.iloc[j][value]

            queries.append(query)

        body = {
            'ubicaciones': queries
        }

        # The query list is sent using the POST version of the resource / location
        resp = requests.post('https://apis.datos.gob.ar/georef/api/ubicacion', json=body)
        resp.raise_for_status()
        results = resp.json()['resultados']

        # A new DataFrame is created with the results of each query as rows
        tmp =  pd.DataFrame(result['ubicacion'] for result in results).drop(columns=['lon', 'lat'])
        geocoded = geocoded.append(tmp)

    geocoded.index = data.index
    return pd.concat([geocoded.add_prefix(prefix), data], axis='columns')


def normalize_and_rename_columns(dataframe):
    """Normalized and renamed columns of the dataframe using georef_reverse_geocode()
    and the parameters of latitude and longitude.
    """
    if str(dataframe) == str(df_museos):
        # added category column
        dataframe['categoría'] = 'Museos'

        # the province column is normalized and the department id 
        # is obtained through the georef_reverse_geocode()
        dataframe = georef_reverse_geocode(dataframe, {'lat': 'latitud', 'lon': 'longitud'})

        # rename columns
        dataframe.drop(['provincia_id', 'provincia', 'gr_departamento_nombre',
                        'gr_municipio_id', 'gr_municipio_nombre'],
                        axis=1, inplace=True)

        dataframe.rename(columns= {
            'localidad_id' : 'cod_localidad',
            'gr_provincia_nombre' : 'provincia',
            'gr_provincia_id' : 'id_provincia',
            'gr_departamento_id' : 'id_departamento',
            'direccion' : 'domicilio',
            'codigo_postal' : 'código postal',
            'telefono' : 'número de teléfono'
        }, inplace=True)

    elif str(dataframe) == str(df_cines):
        dataframe = georef_reverse_geocode(dataframe, {'lat': 'Latitud', 'lon': 'Longitud'}) 

        dataframe.drop(['IdProvincia', 'Provincia', 'IdDepartamento',
                        'gr_municipio_id', 'gr_municipio_nombre',
                        'gr_departamento_nombre'],
                        axis=1, inplace=True)

        dataframe.rename(columns= {
            'Cod_Loc' : 'cod_localidad',
            'gr_provincia_nombre' : 'provincia',
            'gr_provincia_id' : 'id_provincia',
            'gr_departamento_id' : 'id_departamento',
            'Localidad' : 'localidad',
            'Dirección' : 'domicilio',
            'CP' : 'código postal',
            'Teléfono' : 'número de teléfono',
            'Categoría' : 'categoría',
            'Latitud' : 'latitud',
            'Longitud' : 'longitud',
            'Observaciones': 'observaciones',
            'Piso': 'piso',
            'Pantallas' : 'pantallas',
            'Butacas' : 'butacas',
            'espacio_INCAA' : 'espacios INCAA',
            'Mail' : 'mail',
            'Web' : 'web',
            'Nombre' : 'nombre',
            'Fuente' : 'fuente'
        }, inplace=True)

    elif str(dataframe) == str(df_bibliotecas):
        dataframe = georef_reverse_geocode(df_bibliotecas, {'lat': 'Latitud', 'lon': 'Longitud'})

        dataframe.drop(['IdProvincia', 'Provincia', 'IdDepartamento', 'gr_municipio_id',
                        'gr_municipio_nombre', 'gr_departamento_nombre'],
                        axis=1, inplace=True)

        dataframe.rename(columns= {
            'Cod_Loc' : 'cod_localidad',
            'gr_provincia_nombre' : 'provincia',
            'gr_provincia_id' : 'id_provincia',
            'gr_departamento_id' : 'id_departamento',
            'Domicilio' : 'domicilio',
            'Localidad' : 'localidad',
            'CP' : 'código postal',
            'Teléfono' : 'número de teléfono',
            'Categoría' : 'categoría',
            'Observacion' : 'observaciones',
            'Piso': 'piso',
            'Mail' : 'mail',
            'Web' : 'web',
            'Nombre': 'nombre',
            'Fuente' : 'fuente'
        }, inplace=True)

    return dataframe


if __name__ == "__main__":
    start = time.time()

    download_data_files()

    df_museos = read_file_csv('museos')
    df_cines = read_file_csv('cines')
    df_bibliotecas = read_file_csv('bibliotecas')

    df_museos_norm = normalize_and_rename_columns(df_museos)
    df_cines_norm = normalize_and_rename_columns(df_cines)
    df_bibliotecas_norm = normalize_and_rename_columns(df_bibliotecas)
    
    end = time.time()
    print(end-start)
