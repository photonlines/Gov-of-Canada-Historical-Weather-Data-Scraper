import calendar
import os

import requests
from bs4 import BeautifulSoup
import pandas as pd
import numbers
import re
import datetime
import pyodbc
import logging
import pprint

from collections import namedtuple
from sqlalchemy import Table, MetaData, Column, Integer, engine, create_engine, String, DECIMAL, text, update, table
from io import StringIO
from sqlalchemy.exc import IntegrityError

# Database info to use in instances where we want to insert the data into a database.

SQLLITE_DATABASE_PATH = 'C:\\SQLLite\\gc_ca_climate_data.db'
SQLLITE_ODBC_DRIVER = 'SQLite3 ODBC Driver'

ODBC_CONNECTION_STRING =   'Driver=' + SQLLITE_ODBC_DRIVER + ';' \
                         + 'Database=' + SQLLITE_DATABASE_PATH + ';'

DATABASE_ENGINE_STRING = "sqlite:///" + SQLLITE_DATABASE_PATH
DATABASE_TABLE_NAME = 'gov_of_canada_weather_data'

LOG_FILE_LOCATION = 'database_log.log'

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

# Function used to fetch and return a default value for our mapped climate data.
def get_default_data_value(column_name):

    # If the data has a default entry in our map, we return the mapped value, otherwise, return a default value of 0
    return {
          'station_name' : ''
        , 'city' : ''
        , 'province' : ''
        , 'monthly_data_url' : ''
    }.get(column_name, 0)

# A dictionary / map  mapping a month number to a month name. i.e: 1 : January, 2 : February ... 12 : December
month_name_map = {month_number: month_name for month_number, month_name in enumerate(calendar.month_name)}

# A province name to province code mapping which maps an actual province name to a code we can use to
# generate our climate data url input parameters
province_map = {
      "ALBERTA" : "AB"
    , "BRITISH COLUMBIA" : "BC"
    , "MANITOBA" : "MB"
    , "NEW BRUNSWICK" : "NB"
    , "NEWFOUNDLAND AND LABRADOR" : "NL"
    , "NORTHWEST TERRITORIES": "NT"
    , "NOVA SCOTIA": "NS"
    , "NUNAVUT" : "NT"
    , "ONTARIO" : "ON"
    , "PRINCE EDWARD ISLAND" : "PE"
    , "QUEBEC" : "QC"
    , "SASKATCHEWAN": "SK"
    , "YUKON" : "YT"
}

def is_integer(value):
    try:
        int(value)
        return True
    except ValueError:
        return False

def is_float(value):
    try:
        float(value)
        return True
    except:
        return False

# Attempt to cast the passed in value into a numeric format and return the results.
def try_to_convert_to_numeric(value):

    value = replace_all_non_ASCII( value )

    if is_integer(value):
        return int(value)
    elif is_float(value):
        return float(value)
    else:

        # If the value is a string starting with a numeric value followed by a substring containing 'legend'
        # extract the starting numeric value and return it:
        match = re.match(r"([+-]?(\d*\.)?\d+)(.*?)Legend(.*?)",value)

        if match:
            return match.group(1)

        return value

def is_non_zero_numeric_value( value ):
    return is_a_number(value) and value != 0

def convert_list_items_to_numeric( list ):
    return [ try_to_convert_to_numeric(item) for item in list ]

def is_a_number(value):
    return isinstance(value, numbers.Number)

def has_digits(value):
    return is_a_number(value) or any(char.isdigit() for char in value)

def has_non_zero_digits(value):
    return is_non_zero_numeric_value(value) or any((char.isdigit() and char != '0') for char in str(value))

# Find and return the string between the start_substring character(s) and end substring character(s)
def find_string_between ( string, start_substring, end_substring ):
    try:
        return re.search( start_substring + '(.+?)' + end_substring, string ).group(1)
    except AttributeError:
        return  ''

# Replace all of the non-ASCII characters in the input string with the replacement string and return the result
def replace_all_non_ASCII(string, replacement_string =''):

    if (isinstance(string, str)):
        return ''.join([i if ord(i) < 128 else replacement_string for i in string])

    return string

has_numeric_data = lambda data : has_digits(data)

# An exception we throw if no geographic location with our input city / province mapping exists
class NoLocationFoundError(Exception):
    pass

def get_station_data_from_ftp( ftp_file_url = 'ftp://client_climate@ftp.tor.ec.gc.ca/Pub/Get_More_Data_Plus_de_donnees/Station%20Inventory%20EN.csv'):
    return pd.read_csv(ftp_file_url, skiprows = 3)

# Return the government of canada url containing daily data for a specified station / year / month
def get_gc_ca_daily_data_url():
    return "http://climate.weather.gc.ca/climate_data/daily_data_e.html?&StationID=&Prov=&Month=&Year="

# Fetch a pandas data frame containing the city and province names along with the station IDs mapped to the
# city/province. We use the station IDs as part of the URL parameters used to scrape the weather data.
def get_station_data( city, province = ''):

    data = get_station_data_from_ftp()

    # Construct the pandas query which fetches locations that contain the city name followed by a whitespace and
    # locations which match the city name:
    pandas_query = '(Name.str.contains("' + city.upper() + ' ") or Name == "' + city.upper() + '")'

    # If the province info is provided, add in additional province filtering to our query
    if province:
        pandas_query = pandas_query + ' and ( Province == "' + province.upper() + '")'

    # Perform the pandas query to fetch the city / province data we're looking for:
    city_data = data.query(pandas_query)[["Name", "Station ID", "Province"]]

    # Throw an exception if no location data is found:
    if len(city_data) <= 0:
        raise NoLocationFoundError

    return city_data

# This function scrapes the government of Canada climate data for historical climate data for the provided
# city / province. It fetches the data which extends from and includes the start year / start month  to the
# end year / end month.
# Arguments:
# city -- The city we want to query
# province -- The province our city is located in (optional)
# start_year -- The start year (defaults to current year)
# start_month -- Numeric start month (i.e. 1 = January, 2 = February, etc...) (defaults to 1 / January)
# end_year -- The end year (defaults to current year)
# end_month -- Numeric end month (i.e. 1 = January, 2 = February, etc...) (defaults to 12 / December )
# fetch_all_station_data -- Boolean argument which indicates whether we want to include the data for every station
#                           located within our city / province. If set to True, the function returns all of the
#                           station data for our City. Otherwise, it attempts to fetch data from a station which
#                           contains the largest amount of climate information (defaults to True)
# output_processing_steps -- Boolean flag which, if set to True, outputs the processing steps our function steps
#                            through as it fetches / scrapes the climate data (defaults to True)
#
# Returns:
# climate_data_map -- A dictionary containing a collection of named tuples with the scraped climate data.
#                     It maps a key containing a (station_name, station_id, year, month, day) tuple to a named tuple
#                     object containing the scraped daily climate data for the station / city.
def get_climate_data_map (   city
                           , province = ""
                           , start_year = datetime.datetime.now().year
                           , start_month = 1
                           , end_year = datetime.datetime.now().year
                           , end_month = 12
                           , fetch_all_station_data = True
                           , output_processing_steps = True
                           ):

    # Get the location data station info and load it into a pandas data frame
    station_data = get_station_data(city, province)

    # Initialize the climate data map we'll use to store the historical climate info
    climate_data_map = {}

    # Iterate through each year / month starting from the specified start year / start month to the
    # end year / end month:
    for year in range(start_year, end_year + 1):
        for month in range(start_month if year == start_year else 1, end_month + 1 if year == end_year else 13):

            # Iterate through each station:
            for index, row in station_data.iterrows():

                # Construct the URL we need to use to fetch the daily data for the year / month / station we're
                # currently processing:

                daily_data_url = get_gc_ca_daily_data_url()

                daily_data_url = daily_data_url.replace("StationID=", "StationID=" + str(row["Station ID"]))
                daily_data_url = daily_data_url.replace("Prov=", "Prov=" + province_map[row["Province"]])
                daily_data_url = daily_data_url.replace("Month=", "Month=" + str(month))
                daily_data_url = daily_data_url.replace("Year=", "Year=" + str(year))

                # Fetch the data from our URL
                r = requests.get(daily_data_url)
                data = r.text

                if output_processing_steps:

                    print(  "Scraping Climate Data For: "
                          + "Station ID: " + str(row["Station ID"]) + " ; "
                          + "Station Name: " + row["Name"] + " ; "
                          + "Year: " + str(year) + " ; "
                          + "Month: " + str(month) + " ; "
                          + "URL: " + daily_data_url
                          )

                # Initialize the beautiful soup parser we're going to use to parse the returned HTML page
                soup = BeautifulSoup(data, features="html.parser")

                # Construct the page caption text to check and make sure that the returned page contains the
                # data we're looking for:
                caption_text = "Daily Data Report for " + month_name_map[month] + " " + str(year)

                # Iterate through each caption element in our HTML text and check if it has the caption date
                # info we're looking for:
                for caption in soup.find_all('caption'):
                    if caption_text in caption.get_text():

                        # If the caption text matches our date description, fetch the parent table containing our
                        # caption:
                        table = caption.find_parent('table')

                        # Construct the column header info / data we're going to use to store our climate data
                        # objects. We do this by going through the HTML table header info, processing the
                        # column header info, and creating a generic column name for each column, as well as
                        # adding in any additional info we will want to store for each processed entry (including
                        # year, month, day info, as well as the url info:

                        column_header_list = []

                        column_header_list.append('year')
                        column_header_list.append('month')
                        column_header_list.append('day')

                        # For each table header element
                        for thead in table.findAll('thead'):

                            # For each link present within our table head element
                            for link in thead.findAll('a'):

                                # Find the first 'abbr' tag / element
                                abbr = link.find('abbr')

                                # If we find that an 'abbr' tag / element exists, use the text within this element
                                # to store our header text. Otherwise, use the link text info to store the header text:
                                if abbr:
                                    column_header_text = abbr.text
                                else:
                                    column_header_text = link.text

                                if column_header_text:

                                    # Strip out all of the leading / ending white spaces, replace all of the remaining
                                    # white space with underscore characters, and convert any upper case characters
                                    # to lower case from the column header text prior to adding it to our header list
                                    column_header_list.append( column_header_text
                                                               .strip()
                                                               .replace(' ', '_')
                                                               .lower()
                                                             )

                        column_header_list.append('monthly_data_url')

                        column_header_list.insert(0, 'station_name')
                        column_header_list.insert(0, 'station_id')
                        column_header_list.insert(0, 'province')
                        column_header_list.insert(0, 'city')

                        # Construct the named tuple object we're going to use to store the data. Use the column
                        # header list we just constructed to define the tuple object
                        ClimateData = namedtuple('ClimateData', column_header_list)

                        # Fetch all of the table row info excluding the first row
                        all_rows_except_first = table.find_all('tr') [2 : ]

                        # Function which checks whether the html string is a table header or summary row
                        is_table_header_or_summary_row = lambda table_row: table_row.findChildren('th')

                        # Fetch only the climate data rows (any rows excluding header / summary info)
                        data_rows = [table_row for table_row in all_rows_except_first if not is_table_header_or_summary_row(table_row)]

                        for table_row in data_rows:

                            row_data = table_row.find_all('td')

                            row_data = [td.text for td in row_data]

                            # Find all of the positive integer value entries from the leading column text
                            all_int_entries = re.findall(r'\d+', row_data[0])

                            # If the first column contains a leading digit, we extract it and assume it represents
                            # the month day we want to process, and we process the row data:
                            if all_int_entries:

                                day = int(all_int_entries[0])

                                # Insert the date data info into our row data:

                                row_data.insert(0, month)
                                row_data.insert(0, year)

                                # Convert any numeric row data stored as text into numeric format
                                row_data = convert_list_items_to_numeric(row_data)

                                row_data.append( daily_data_url )

                                station_name = row["Name"]
                                province = row["Province"]
                                station_id = row["Station ID"]

                                if fetch_all_station_data:

                                    # Add in the location and station data to our data list:

                                    row_data.insert(0, station_name)
                                    row_data.insert(0, station_id)
                                    row_data.insert(0, province)
                                    row_data.insert(0, city)

                                    # Create a climate data tuple containing our scraped climate info and add it
                                    # to our climate data map

                                    climate_data = ClimateData._make( row_data )
                                    climate_data_map[(station_name, station_id, year, month, day)] = climate_data

                                else:

                                    # Add in the location and default station data to our data list:

                                    row_data.insert(0, '')
                                    row_data.insert(0, 0)
                                    row_data.insert(0, province)
                                    row_data.insert(0, city)

                                    # Here, we only include the climate data if it has enough information. If the current
                                    # item has more info than our previous entry, we use this data and scrap our old
                                    # entry:

                                    if ((year, month, day) in climate_data_map):

                                        current_climate_data = climate_data_map[(year, month, day)]

                                        # If we currently have data for this day, we compare the number of numeric
                                        # columns we have compared to the new data on hand. If the new data
                                        # contains more info than our old info, we replace the old column info / data
                                        # with our new tuple data.

                                        new_numeric_data = [item for item in row_data if has_non_zero_digits(item)]

                                        current_climate_numeric_data \
                                            = [item for item in current_climate_data if has_non_zero_digits(item)]

                                        if len(new_numeric_data) > len(current_climate_numeric_data):
                                            climate_data = ClimateData._make(row_data)
                                            climate_data_map[(year, month, day)] = climate_data

                                    else:

                                        # Create a climate data tuple containing our scraped climate info and add it
                                        # to our climate data map

                                        climate_data = ClimateData._make(row_data)
                                        climate_data_map[(year, month, day)] = climate_data

    return climate_data_map

# A pretty printer class we use to print tuples. It allows us to output our processing steps in an
# easy to read manner should we require to do so.
class TuplePrettyPrinter(pprint.PrettyPrinter):

    def format_namedtuple(self, object, stream, indent, allowance, context, level):

        write = stream.write
        write(object.__class__.__name__ + '(')
        object_dict = object._asdict()

        length = len(object_dict)

        if length:

            # Attempt to print the information inline. If it is too large, print it on multiple lines:

            inline_stream = StringIO()

            self.format_namedtuple_items(object_dict.items(), inline_stream, indent, allowance + 1, context, level
                                         , inline = True)

            if len(inline_stream.getvalue()) > (self._width - indent - allowance):
                self.format_namedtuple_items(object_dict.items(), stream, indent, allowance + 1, context, level
                                             , inline = False)

            else:

                stream.write(inline_stream.getvalue())

        write(')')

    def format_namedtuple_items(self, items, stream, indent, allowance, context, level, inline=False):

        indent += self._indent_per_level
        write = stream.write
        last_index = len(items) - 1

        if inline:
            delimnl = ', '

        else:
            delimnl = ',\n' + ' ' * indent
            write('\n' + ' ' * indent)

        for i, (key, ent) in enumerate(items):
            last = i == last_index
            write(key + '=')

            self._format(ent, stream, indent + len(key) + 2, allowance if last else 1, context, level)

            if not last:

                write(delimnl)

    def _format(self, object, stream, indent, allowance, context, level):

        # Dynamically add the types of our namedtuple and namedtuple like classes to the _dispatch object of pprint
        # which maps classes to formatting methods:

        if hasattr(object, '_asdict') and type(object).__repr__ not in self._dispatch:
            self._dispatch[type(object).__repr__] = TuplePrettyPrinter.format_namedtuple

        super()._format(object, stream, indent, allowance, context, level)

# Pretty print the input climate data map / dictionary
def pretty_print_climate_data_map(climate_data_map):

    pretty_printer = TuplePrettyPrinter()

    for key, value in climate_data_map.items():

        pretty_printer.pprint(value)

# Construct a dictionary / map containing info on mapping the passed in tuple data to our database table.
# Use the tuple object names as the default column names which map to the database table column names. If any of the
# table columns are missing from the input tuple, we map default values to the given entries and return a
# dictionary object which contains the table column -> data info we can use to insert / update our data.
def construct_column_insert_map_from_tuple( tuple, database_table_name ):

    # Convert the tuple containing our climate data into a dictionary / map object
    column_map = tuple._asdict()

    tuple_column_name_list = list(column_map.keys())

    # Fetch a list of all column names contained in our database table
    sql_column_name_list = get_column_names_from_database_table( database_table_name, ODBC_CONNECTION_STRING )

    # Check if we're missing any data, and if we are, assign a default value to the missing columns / items:
    missing_column_entries = [item for item in sql_column_name_list if item not in tuple_column_name_list]

    for column_name in missing_column_entries:
        column_map[column_name] = get_default_data_value(column_name)

    # If our data is mapped to entries which contain one of the below sub-strings, remove the data from our
    # map and replace it with 0:

    list_of_strings_to_check_for = ['LegendMM', 'LegendTT', r'\xa']

    for key, value in column_map.items():

        if str(value) == '' or any(sub_string in str(value) for sub_string in list_of_strings_to_check_for):
            column_map[key] = 0

    return column_map

# Return a sql statement which can be used to query a SQLLite database for a list of columns names contained
# within a database table
def get_sql_lite_schema_select_query( database_table_name ):
    return "SELECT sql FROM sqlite_master where tbl_name = '" \
                               + database_table_name + "'" \
                               + 'AND type = \'table\''

# Return a sql statement which can be used to query a SQL Server database for a list of columns names contained
# within a database table
def get_sql_server_schema_select_query( database_table_name ):
    return "select column_name from information_schema.columns where table_name = '" \
                               + database_table_name + "'"

# Construct and return a list of column names contained in the database table. The information is accessed through the
# passed in ODBC connection string.
def get_column_names_from_database_table( database_table_name, odbc_connection_string ):

    connection = pyodbc.connect(odbc_connection_string)

    # Query the database schema information for a list of column names for our table:
    column_name_schema_select_query = get_sql_lite_schema_select_query( database_table_name )

    cursor = connection.cursor()
    cursor.execute(column_name_schema_select_query)
    records = cursor.fetchall()

    # Iterate through the list of column names and add each name to our list:
    column_name_list = []

    for column in records:
        column_name_list.append(find_string_between(str(column), "'", "'"))

    connection.close()

    return column_name_list

# Generate an update table script for the passed in column map and table object. The column map is a dictionary
# containing a mapping of our table column names to new column values. The sql table is a SQLAlchemy object
# containing our database table info.
def generate_update_table_sql( column_map, sql_table ):

    # Create a column name to column value mapping for all of the columns in our sql_table which are not part of
    # the table primary key
    update_column_dict = {c.name: str( column_map.get(c.name, get_default_data_value(c.name))).replace('\'', '\'\'')
                          for c in sql_table.c
                          if not c.primary_key}

    # Create a where filter dictionary / map to use to map our primary key data. It's used to construct the where
    # clause needed in order to generate the update statement needed to update the data we want to update
    where_column_dict = {c.name: str(column_map[c.name]).replace('\'', '\'\'')
                         for c in sql_table.c
                         if c.primary_key}

    # Generate an appropriate update statement for our column map and table:

    update_statement = "UPDATE " + sql_table.name
    set_statement = " SET " + ', '.join("{!s} = {!r}".format(key, value) for (key, value) in update_column_dict.items())
    where_statement = " WHERE " + ' AND '.join(
        "{!s} = {!r}".format(key, value) for (key, value) in where_column_dict.items())

    where_statement = where_statement.replace('"', "'")

    update_sql_statement = update_statement + set_statement + where_statement

    return update_sql_statement

# Insert the passed in climate data mapping into a database
# Arguments:
# climate_data_map -- A dictionary containing our mapped climate data.
# database_engine_string -- The string we use to produce our SQLAlchemy engine object used to connect to our database
#                           The typical form of a database URL to connect is:
#                           dialect+driver://username:password@host:port/database
# database_table_name -- The name of the database table to create / insert our data into.
# drop_and_create_table -- Boolean flag used to control whether we want to drop and insert our table. If set to
#                          true, the table along with any data contained within the table with our database_table_name
#                          will be dropped and re-inserted. If set to False, we'll simply use the existing table and
#                          try to either insert or update the climate data.
def insert_climate_data_into_database(   climate_data_map
                                       , database_engine_string
                                       , database_table_name
                                       , drop_and_create_table = False ):



    eng = create_engine(database_engine_string)

    meta_data = MetaData()

    sql_table = Table(  database_table_name
                      , meta_data
                      , Column('city', String(200), primary_key = True)
                      , Column('province', String(100))
                      , Column('station_id', Integer)
                      , Column('station_name', String(200), primary_key = True)
                      , Column('monthly_data_url', String(500))
                      , Column('year', Integer, primary_key = True)
                      , Column('month', Integer, primary_key = True)
                      , Column('day', Integer, primary_key = True)
                      , Column('max_temp', DECIMAL(5, 2))
                      , Column('min_temp', DECIMAL(5, 2))
                      , Column('mean_temp', DECIMAL(5, 2))
                      , Column('extr_max_temp', DECIMAL(5, 2))
                      , Column('extr_min_temp', DECIMAL(5, 2))
                      , Column('cool_deg_days', DECIMAL(5, 2))
                      , Column('heat_deg_days', DECIMAL(5, 2))
                      , Column('total_rain', DECIMAL(5, 2))
                      , Column('total_snow', DECIMAL(5, 2))
                      , Column('total_prec', DECIMAL(5, 2))
                      , Column('snow_on_grnd', DECIMAL(5, 2))
                      , Column('dir_of_max_gust', DECIMAL(5, 2))
                      , Column('spd_of_max_gust', String(30))
                      )

    if drop_and_create_table:

        # Drop the existing database table if it exists and re-create it:

        drop_table_sql = text("DROP TABLE IF EXISTS " + database_table_name)
        eng.execute( drop_table_sql )

        meta_data.create_all(eng)

    for key, value in climate_data_map.items():

        column_map = construct_column_insert_map_from_tuple(value, database_table_name)

        # Try to insert the climate data into the database table. If any issues are encountered or if data with
        # the given key is already mapped / available, attempt to update the data instead:
        try:

            eng.execute(sql_table.insert(), column_map)

        except IntegrityError:

            # If an integrity error is encountered, it means that the table already has an entry for our record, so
            # try to generate an update statement instead and attempt to execute it:

            update_sql_statement = generate_update_table_sql( column_map, sql_table)
            eng.execute(update_sql_statement)

if __name__ == "__main__":

    # Fetch the historical climate data for all Toronto stations starting from January 2018 and ending in Feb 2019:
    climate_data_map = get_climate_data_map(
          city = 'Toronto'
        , province = ''
        , start_year = 2018
        , start_month = 1
        , end_year = 2019
        , end_month = 2
        , fetch_all_station_data = True
    )

    # Insert the data into a new SQLLite3 database with our initialized parameters (currently mapped to
    # the database path provided in SQLLITE_DATABASE_PATH global variable and using a SQLLite3 ODBC driver):
    insert_climate_data_into_database (
          climate_data_map = climate_data_map
        , database_engine_string = DATABASE_ENGINE_STRING
        , database_table_name = DATABASE_TABLE_NAME
        , drop_and_create_table = True
    )


