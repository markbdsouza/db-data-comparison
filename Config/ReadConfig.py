import json
from operator import itemgetter

from Config.CONSTANTS import CONFIG


def read_input_queries_list():
    """Read Comparison Queries List"""
    with open(CONFIG) as f:
        list_of_query_dictionary = json.load(f)['ComparisonQueries']
    return list_of_query_dictionary


def read_mysql_config_variables():
    """Read MY SQL Connection Details"""
    with open(CONFIG) as f:
        connection_vars = json.load(f)['MySQL']
    return itemgetter('HostName', 'UserName', 'Password', 'Schema')(connection_vars)


def read_oracle_connection_variables():
    """Read Oracle Connection Details"""
    with open(CONFIG) as f:
        connection_vars = json.load(f)['Oracle']
    return itemgetter('HostName', 'Port', 'ServiceName', 'UserName', 'Password')(connection_vars)


def read_csv_connection_variables():
    """Read File Name for CSV File Sources"""
    with open(CONFIG) as f:
        connection_vars = json.load(f)['CSV']
    return itemgetter('FileName')(connection_vars)


def read_txt_connection_variables():
    """Read File Name and Delimiter for TXT File Sources"""
    with open(CONFIG) as f:
        connection_vars = json.load(f)['TXT']
    return itemgetter('FileName', 'Delimiter')(connection_vars)
