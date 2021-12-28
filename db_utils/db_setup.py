from db_utils import params_config
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def create_db():
    conn = psycopg2.connect(
        user=params_config.params['user'],
        password=params_config.params['password'], 
        host=params_config.params["host"]
    )

    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    pgcursor = conn.cursor()
    pgcursor.execute('DROP DATABASE IF EXISTS {}'.format(params_config.params['dbname']))
    pgcursor.execute('CREATE DATABASE {}'.format(params_config.params['dbname']))
    
    conn.close()
    
