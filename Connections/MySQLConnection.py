import json
import pandas as pd
from sqlalchemy import create_engine,exc
from operator import itemgetter

# https://hackersandslackers.com/compare-rows-pandas-dataframes/
def read_oracle_config_variables():
    with open('config.json') as f:
        connection_vars = json.load(f)['MySQL']
    return itemgetter('HostName', 'UserName', 'Password', 'Schema')(connection_vars)


def read_my_sql_query():
    with open('config.json') as f:
        connection_vars = json.load(f)['MySQL']
    return itemgetter('Query')(connection_vars)


def create_my_sql_connection():
    host_name, user_name, password, schema = read_oracle_config_variables()
    sqlEngine = create_engine('mysql+pymysql://{}:{}@{}/{}'.format(user_name, password, host_name,schema), pool_recycle=3600)
    return sqlEngine.connect()


def create_data_frame(query, table_name = ''):
    try:
        my_sql_connection = create_my_sql_connection()
        # query = read_my_sql_query() if table_name == '' else  "select * from {}".format(table_name)
        data_frame = pd.read_sql(query, my_sql_connection)
        close_connection(my_sql_connection)
        return data_frame
    except RuntimeError as e:
        print('Runtime Error: {}'.format(e))
    except exc.OperationalError as e:
        print('Operational Error: {}'.format(e))


def close_connection(con):
    con.close();


def test_connection():
    df = create_data_frame()
    print(df)


if __name__=="__main__":
    print("inside")
    test_connection()
