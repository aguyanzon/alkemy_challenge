from db_utils import params_config
import logging
from psycopg2 import OperationalError
from sqlalchemy import create_engine


# Using alchemy method
connect_alchemy = "postgresql+psycopg2://%s:%s@%s/%s" % (
    params_config.params['user'],
    params_config.params['password'],
    params_config.params['host'],
    params_config.params['dbname']
)

def using_alchemy(df, table_name):
    try:
        engine = create_engine(connect_alchemy)
        # convert dataframe to sql
        df.to_sql(table_name, con=engine, index=False, if_exists='replace')
        logging.info("Data inserted using to_sql()(sqlalchemy) done successfully...")

    except OperationalError as error:
        # passing exception to function
        logging.critical(f"Unexpected {error= }, {type(error)= }")



