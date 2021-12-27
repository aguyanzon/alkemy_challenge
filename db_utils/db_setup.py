import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine
import decouple

params = {
    'user': decouple.config("DB_USER"),
    'password': decouple.config("DB_PASSWORD"),
    'host': decouple.config("DB_HOST"),
    'dbname' : decouple.config("DB_NAME")
}

def create_db():
    conn = psycopg2.connect(user=params['user'], password=params['password'], host=params["host"])
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    pgcursor = conn.cursor()
    pgcursor.execute('DROP DATABASE IF EXISTS {}'.format(params['dbname']))
    pgcursor.execute('CREATE DATABASE {}'.format(params['dbname']))
    conn.close()
    
