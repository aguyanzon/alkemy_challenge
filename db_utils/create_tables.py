from db_utils import params_config
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

  
def connect(file_name):
    conn = psycopg2.connect(**params_config.params)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    sql_file = open(file_name, 'r')
    cursor.execute(sql_file.read())

    return cursor