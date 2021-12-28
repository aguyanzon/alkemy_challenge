import pandas as pd
import psycopg2
from psycopg2 import OperationalError, errorcodes, errors
from sqlalchemy import create_engine
from datetime import date
import decouple
import logging

params = {
    'user': decouple.config("DB_USER"),
    'password': decouple.config("DB_PASSWORD"),
    'host': decouple.config("DB_HOST"),
    'dbname' : decouple.config("DB_NAME")
}

# Using alchemy method
connect_alchemy = "postgresql+psycopg2://%s:%s@%s/%s" % (
    params['user'],
    params['password'],
    params['host'],
    params['dbname']
)

def using_alchemy(df, table_name):
    try:
        engine = create_engine(connect_alchemy)
        df.to_sql(table_name, con=engine, index=False, if_exists='replace')
        logging.info("Data inserted using to_sql()(sqlalchemy) done successfully...")
    except OperationalError as error:
        # passing exception to function
        logging.critical(f"Unexpected {error= }, {type(error)= }")



