import cx_Oracle
import json
import pandas as pd
from sqlalchemy import create_engine
from operator import itemgetter


def read_oracle_connection_variables():
    with open('config.json') as f:
        connection_vars = json.load(f)['Oracle']
    return itemgetter('HostName', 'Port', 'ServiceName', 'UserName', 'Password')(connection_vars)


def read_oracle_query():
    with open('config.json') as f:
        connection_vars = json.load(f)['Oracle']
    return itemgetter('query')(connection_vars)


def create_ctx_oracle_connection():
    host_name, port, service_name, user_name, password = read_oracle_connection_variables()
    with open('config.json') as f:
        connection_vars = json.load(f)['Oracle']
    dsn_tns = cx_Oracle.makedsn(host_name, port,service_name)
    return cx_Oracle.connect(user_name, password, dsn_tns)



def create_sql_alchemy_connection():
    host_name, port, service_name, user_name, password = read_oracle_connection_variables()
    con_string = "oracle+cx_oracle://{0}:{1}@{2}:{3}/{4}?encoding=UTF-8".format(user_name, password, host_name, str(port), service_name)
    e = create_engine(con_string)
    return e.connect()


def create_data_frame(table_name = ""):
    try:
        con = create_sql_alchemy_connection()
        if table_name != "":
            query = "select * from {}".format(table_name)
        else:
            query= read_oracle_query()
        frame = pd.read_sql(query, con)
        close_connection(con)
        return frame
    except: # catch *all* exceptions
        print('Error occurred while connecting to Oracle')
    # finally:



def close_connection(con):
    con.close();


def test_connection():
    create_sql_alchemy_connection()

test_connection()
