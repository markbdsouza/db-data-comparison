import json
from sqlalchemy import create_engine,exc
from operator import itemgetter

# https://hackersandslackers.com/compare-rows-pandas-dataframes/
from Connections.ReadConfig import read_oracle_config_variables
from DataframeLogic.GenericDFActivities import create_data_frame_with_sql_con


def create_my_sql_connection():
    host_name, user_name, password, schema = read_oracle_config_variables()
    sqlEngine = create_engine('mysql+pymysql://{}:{}@{}/{}'.format(user_name, password, host_name,schema), pool_recycle=3600)
    return sqlEngine.connect()


def create_data_frame(query):
    try:
        my_sql_connection = create_my_sql_connection()
        data_frame = create_data_frame_with_sql_con(query, my_sql_connection)
        close_connection(my_sql_connection)
        return data_frame
    except RuntimeError as e:
        print('Runtime Error: {}'.format(e))
    except exc.OperationalError as e:
        print('Operational Error: {}'.format(e))


def close_connection(con):
    con.close();


def test_connection():
    df = create_data_frame("select * from actor")
    print(df)


if __name__=="__main__":
    test_connection()
