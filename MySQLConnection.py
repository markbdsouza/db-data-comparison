import json
import pandas as pd
from sqlalchemy import create_engine,exc
from operator import itemgetter

# https://hackersandslackers.com/compare-rows-pandas-dataframes/
def read_oracle_config_variables():
    with open('config.json') as f:
        connection_vars = json.load(f)['MySQL']
    return itemgetter('HostName', 'UserName', 'Password', 'Schema')(connection_vars)


def create_my_sql_connection():
    host_name, user_name, password, schema = read_oracle_config_variables()
    sqlEngine = create_engine('mysql+pymysql://{}:{}@{}'.format(user_name, password, host_name), pool_recycle=3600)
    return sqlEngine.connect()


def create_data_frame(table_name = 'sakila.actor'):
    my_sql_connection = create_my_sql_connection()
    data_frame = pd.read_sql("select * from {}".format(table_name), my_sql_connection)
    close_connection(my_sql_connection)
    return data_frame


def close_connection(con):
    con.close();


def test_connection():
    create_data_frame()


test_connection()
