import logging
import os

import decouple
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database


params = "postgresql+psycopg2://{}:{}@{}/{}".format(
    decouple.config("DB_USER"),
    decouple.config("DB_PASSWORD"),
    decouple.config("DB_HOST"),
    decouple.config("DB_NAME")
)
engine = create_engine(params, pool_size=1)


def create_db():
    if not database_exists(engine.url):
        create_database(engine.url)


def create_tables():
    sql_scripts_dir = os.path.join(os.getcwd() , "sql_scripts")
    for file_name in os.listdir(sql_scripts_dir):
        sql_file = open(os.path.join(sql_scripts_dir, file_name), 'r')
        engine.execute(sql_file.read())


def insert_dataframe(df, table_name):
    # convert dataframe to sql
    df.to_sql(table_name, con=engine, index=False, if_exists='replace')
    logging.info("Data inserted using to_sql()(sqlalchemy) done successfully...")







