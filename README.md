# Historical Weather Data Scraper for Canada

### Description

This is a Python 3 script which can be used to scrape historical daily weather data from the [Government of Canada website](http://climate.weather.gc.ca/index_e.html). It also provides functionality for inserting the mapped data into a database using [SQLAlchemy](https://www.sqlalchemy.org/). 


### Example Usage

As an example, to get historical weather data for a Toronto from June 2017 until and including February 2019:

```python
climate_data_map = get_climate_data_map(
          city = 'Toronto'
        , province = ''
        , start_year = 2017
        , start_month = 6
        , end_year = 2019
        , end_month = 2
        , fetch_all_station_data = True
    )
```

The above function returns a dictionary mapping a tuple containing the (station_name, station_id, year, month, day) to a named tuple object containing the scraped daily climate data for the mapped station. By default, it fetches all of the station data for the specified city / province, although you can also let it heuristically select only one station to scrape the data from by setting the fetch_all_station_data flag to False. In this instance, it tries to fetch the station data which has the highest information content (most numerical info / entries) for the specified city / province. 

After generating the climate_data_map, we can upsert the data into a database by using the insert_climate_data_into_database function. The example below creates a new SQLLite database / table to hold our data and inserts the climate data into the database:

```python

SQLLITE_DATABASE_PATH = 'C:\\SQLLite\\gc_ca_climate_data.db'
SQLLITE_ODBC_DRIVER = 'SQLite3 ODBC Driver'

ODBC_CONNECTION_STRING =   'Driver=' + SQLLITE_ODBC_DRIVER + ';' \
                         + 'Database=' + SQLLITE_DATABASE_PATH + ';'

DATABASE_ENGINE_STRING = "sqlite:///" + SQLLITE_DATABASE_PATH
DATABASE_TABLE_NAME = 'gov_of_canada_weather_data'

insert_climate_data_into_database (
          climate_data_map = climate_data_map
        , database_engine_string = DATABASE_ENGINE_STRING
        , database_table_name = DATABASE_TABLE_NAME
        , drop_and_create_table = True
    )
```

The drop_and_create_table boolean flag specifies whether we want to drop our climate data table and generate a new one. By default, it's set to False. It should only be set to True if we're sure we want to drop all of our previous data and re-create our climate data table.

Note: the database insertion / update functionality has been tested using SQL Server / SQL Lite. 