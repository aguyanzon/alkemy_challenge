# Alkemy Challenge 

This is a data analytics challenge with python to enter the acceleration provided by [Alkemy](https://www.alkemy.org/).

## Description

* The project takes three csv files from [datos.gob.ar](https://datos.gob.ar/dataset/cultura-mapa-cultural-espacios-culturales/) and downloads them.
* The files are saved in a local directory with their respective name and the date the download was made.
* The data is processed and the information of each one of them is normalized to later create three tables in a Postgresql database.
* A database is created and the tables are created using .sql scripts.
* After the tables are created, the values ​​corresponding to each of them are inserted.
* These tables are updated every time the project is executed.

To see the challenge go to the following [link](https://drive.google.com/file/d/1ZxBnjsof8yCZx1JVLVaq5DbRjvIIvfJs/view).

## Installations steps

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. 

```
git clone git@github.com:aguyanzon/alkemy_challenge.git
pip install virtualenv
python -m virtualenv env
env\Scripts\activate for Windows or source env/bin/activate for Linux/Mac
pip install -r requirements.txt
```

## How to run
In project root directory run:

```
python main.py 
```

## Postgresql configuration

The .env file contains the parameters to access the database. Change them to your own database configuration.

## Configure logging level

The .env file contains a parameter named LOG_LEVEL. Change it to set your own log level value.

![](https://images.ctfassets.net/h6vh38q7qvzk/4ndCwiSGDeEyIqCwmWs2KK/19a7cbf71d36644167a56f95bc1444c0/loggingLevels.jpeg)

## Data normalization

For the normalization of the data an API from the [Argentinian Geographic Data Normalization Service](https://datosgobar.github.io/georef-ar-api/) is used.

In this project, the latitude and longitude data are used to feed the API and it returns the names of the provinces, and in the case of museums, the id of the different departments.

Example:

![](https://raw.github.com/aguyanzon/kaggle/master/resources/example.PNG)



