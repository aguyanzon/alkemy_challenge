import decouple

params = {
    'user': decouple.config("DB_USER"),
    'password': decouple.config("DB_PASSWORD"),
    'host': decouple.config("DB_HOST"),
    'dbname' : decouple.config("DB_NAME")
}