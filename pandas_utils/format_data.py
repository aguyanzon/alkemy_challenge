from datetime import date, datetime
import numpy as np
import pandas as pd
import requests
from urllib3.util.retry import Retry

TODAY = datetime.today()


def read_file_csv(file_name):
    """Read csv files and convert them to dataframe"""
    folder = TODAY.strftime("%Y-%B")
    date_today = TODAY.strftime('%d-%m-%Y')
    df = pd.read_csv(
        f"./{file_name}/{folder}/{file_name}-{date_today}.csv",
        header=1
    )
    
    return df


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
        with requests.Session() as s:
            retries = Retry(
                total= None, 
                status = 5,
                backoff_factor=0.1, 
                status_forcelist=[500],
                allowed_methods= frozenset(['POST']),
                raise_on_status= True   
            )

            s.mount('https://', requests.adapters.HTTPAdapter(max_retries=retries))

            resp = s.post('https://apis.datos.gob.ar/georef/api/ubicacion', json=body)
            results = resp.json()['resultados']

        # A new DataFrame is created with the results of each query as rows
        tmp =  pd.DataFrame(result['ubicacion'] for result in results).drop(columns=['lon', 'lat'])
        geocoded = geocoded.append(tmp)

    geocoded.index = data.index
    return pd.concat([geocoded.add_prefix(prefix), data], axis='columns')


def normalize_and_rename_columns(df_museos, df_cines, df_bibliotecas):
    """Normalize and rename dataframe columns using georef_reverse_geocode()
    and the parameters of latitude and longitude.
    """
    df_dict = {
        'df_museos' : df_museos,
        'df_cines' : df_cines,
        'df_bibliotecas' : df_bibliotecas
    }

    # added category column
    df_dict['df_museos']['categoria'] = 'Museos'

    # the province column is normalized and the department id 
    # is obtained through the georef_reverse_geocode()
 
    df_dict['df_museos'] = georef_reverse_geocode(df_dict['df_museos'], {'lat': 'latitud', 'lon': 'longitud'})

    # rename columns
    df_dict['df_museos'].drop(['provincia_id', 'provincia', 'gr_departamento_nombre',
                    'gr_municipio_id', 'gr_municipio_nombre'],
                    axis=1, inplace=True)

    df_dict['df_museos'].rename(columns= {
        'localidad_id' : 'cod_localidad',
        'gr_provincia_nombre' : 'provincia',
        'gr_provincia_id' : 'id_provincia',
        'gr_departamento_id' : 'id_departamento',
        'direccion' : 'domicilio',
        'codigo_postal' : 'codigo postal',
        'telefono' : 'numero de telefono'
    }, inplace=True)


    df_dict['df_cines'] = georef_reverse_geocode(df_dict['df_cines'], {'lat': 'Latitud', 'lon': 'Longitud'}) 

    df_dict['df_cines'].drop(['IdProvincia', 'Provincia', 'IdDepartamento',
                    'gr_municipio_id', 'gr_municipio_nombre',
                    'gr_departamento_nombre'],
                    axis=1, inplace=True)

    df_dict['df_cines'].rename(columns= {
        'Cod_Loc' : 'cod_localidad',
        'gr_provincia_nombre' : 'provincia',
        'gr_provincia_id' : 'id_provincia',
        'gr_departamento_id' : 'id_departamento',
        'Localidad' : 'localidad',
        'Dirección' : 'domicilio',
        'CP' : 'codigo postal',
        'Teléfono' : 'numero de telefono',
        'Categoría' : 'categoria',
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

    
    df_dict['df_bibliotecas'] = georef_reverse_geocode(df_dict['df_bibliotecas'], {'lat': 'Latitud', 'lon': 'Longitud'})

    df_dict['df_bibliotecas'].drop(['IdProvincia', 'Provincia', 'IdDepartamento', 'gr_municipio_id',
                    'gr_municipio_nombre', 'gr_departamento_nombre'],
                    axis=1, inplace=True)

    df_dict['df_bibliotecas'].rename(columns= {
        'Cod_Loc' : 'cod_localidad',
        'gr_provincia_nombre' : 'provincia',
        'gr_provincia_id' : 'id_provincia',
        'gr_departamento_id' : 'id_departamento',
        'Domicilio' : 'domicilio',
        'Localidad' : 'localidad',
        'CP' : 'codigo postal',
        'Teléfono' : 'numero de telefono',
        'Categoría' : 'categoria',
        'Observacion' : 'observaciones',
        'Piso': 'piso',
        'Mail' : 'mail',
        'Web' : 'web',
        'Nombre': 'nombre',
        'Fuente' : 'fuente'
    }, inplace=True)

    return df_dict


def concat_entities(df_dict):
    return pd.concat([df_dict['df_museos'], df_dict['df_cines'], df_dict['df_bibliotecas']])


def input_table1(df):
    df['id'] = np.arange(1,len(df)+1)
    df['fecha de carga'] = date.today()

    df = df[[
    'id','cod_localidad','id_provincia', 'id_departamento',
    'categoria', 'provincia', 'localidad', 'nombre', 'domicilio',
    'codigo postal', 'numero de telefono', 'mail', 'web',
    'fecha de carga'
    ]] 
    
    return df


def input_table2(df):
    df = df[["provincia", "categoria", "fuente"]]
    data = df.groupby(["provincia","categoria", "fuente"]).agg({
        "categoria":"count",
        "fuente":"count"
        }).rename(
        columns={
            "categoria":"registros categoria",
            "fuente": "registros fuente"
        }).reset_index()

    data['id'] = np.arange(1,len(data)+1)
    data['fecha de carga'] = date.today()

    data = data[["id", "provincia", "categoria", "fuente", "registros categoria", "registros fuente", "fecha de carga"]] 

    return data


def input_table3(df):
    df = df[["provincia", "pantallas", "butacas", "espacios INCAA"]]
    data = df.groupby("provincia").agg({
        "pantallas": "sum",
        "butacas":"sum",
        "espacios INCAA": "count"
        }).reset_index()

    data['id'] = np.arange(1,len(data)+1)
    data['fecha de carga'] = date.today() 

    data = data[["id", "provincia", "pantallas", "butacas", "espacios INCAA", "fecha de carga"]]

    return data

