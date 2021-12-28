import numpy as np
import pandas as pd
from datetime import date

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
    data = df.groupby(["provincia","categoria", "fuente"]).agg(
        {"categoria":"count", "fuente":"count"}).rename(
        columns={"categoria":"registros categoria", "fuente": "registros fuente"}
    ).reset_index()

    data['id'] = np.arange(1,len(data)+1)
    data['fecha de carga'] = date.today()

    data = data[["id", "provincia", "categoria", "fuente", "registros categoria", "registros fuente", "fecha de carga"]] 

    return data


def input_table3(df):
    df = df[["provincia", "pantallas", "butacas", "espacios INCAA"]]
    data = df.groupby("provincia").agg({"pantallas": "sum", "butacas":"sum", "espacios INCAA": "count"}).reset_index()
    data['id'] = np.arange(1,len(data)+1)
    data['fecha de carga'] = date.today() 
    data = data[["id", "provincia", "pantallas", "butacas", "espacios INCAA", "fecha de carga"]]

    return data

