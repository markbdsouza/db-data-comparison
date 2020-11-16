import json
from operator import itemgetter

CONFIG = 'config.json'


def read_input_queries_list():
    with open(CONFIG) as f:
        list_of_query_dictionary = json.load(f)['ComparisonQueries']
    return list_of_query_dictionary


def read_oracle_config_variables():
    with open(CONFIG) as f:
        connection_vars = json.load(f)['MySQL']
    return itemgetter('HostName', 'UserName', 'Password', 'Schema')(connection_vars)


def read_my_sql_query():
    with open(CONFIG) as f:
        connection_vars = json.load(f)['MySQL']
    return itemgetter('Query')(connection_vars)


def read_oracle_connection_variables():
    with open(CONFIG) as f:
        connection_vars = json.load(f)['Oracle']
    return itemgetter('HostName', 'Port', 'ServiceName', 'UserName', 'Password')(connection_vars)


def read_oracle_query():
    with open(CONFIG) as f:
        connection_vars = json.load(f)['Oracle']
    return itemgetter('Query')(connection_vars)


def read_db_details():
    with open(CONFIG) as f:
        config = json.load(f)
        source = config['Source']
        target = config['Target']
    return source,target
