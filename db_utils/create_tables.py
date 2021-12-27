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
  
def connect(file_name):
    conn = psycopg2.connect(**params)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    sqlfile = open(file_name, 'r')
    cursor.execute(sqlfile.read())

    return cursor