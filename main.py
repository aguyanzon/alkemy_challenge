import locale
import logging

from decouple import config

from db_utils import db_utils
from files_utils import files_utils
from pandas_utils import format_data

# to save the folder with the locale language
locale.setlocale(locale.LC_ALL, 'es_ES')

logging.basicConfig(
    format='%(asctime)s: %(levelname)s [%(filename)s] - %(message)s',
    level=config("LOG_LEVEL", cast=int))

# Disable this library's logger because its provides too much unneeded information
logging.getLogger("charset_normalizer").setLevel(logging.CRITICAL)


if __name__ == "__main__":

    '''Downloads files from a website and saves them to a local directory'''
    files_utils.download_data_files()

    """Read csv files and return them as dataframe"""
    df_museos = format_data.csv_to_dataframe('museos')
    df_cines = format_data.csv_to_dataframe('cines')
    df_bibliotecas = format_data.csv_to_dataframe('bibliotecas')

    '''
    The data is processed and a dictionary is returned with the information
    of the normalized dataframes.
    '''
    df_dict = format_data.normalize_and_rename_columns(df_museos, df_cines,
                                                       df_bibliotecas)

    '''Database and tables creation.'''
    db_utils.create_db()

    db_utils.create_tables()

    df_concat = format_data.concat_entities(df_dict)

    '''The information is loaded into the tables'''
    db_utils.insert_dataframe(format_data.input_espacios_culturales(df_concat),
                              'espacios_culturales')
    db_utils.insert_dataframe(format_data.input_registros(df_concat),
                              'registros')
    db_utils.insert_dataframe(format_data.input_cines(df_dict["df_cines"]),
                              'cines')

    logging.info("Information loaded into the database successfully!")