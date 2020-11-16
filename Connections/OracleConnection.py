import cx_Oracle
from sqlalchemy import create_engine
from Connections.ReadConfig import read_oracle_connection_variables
from DataframeLogic.GenericDFActivities import create_data_frame_with_sql_con


# def create_ctx_oracle_connection():
#     host_name, port, service_name, user_name, password = read_oracle_connection_variables()
#     with open('../config.json') as f:
#         connection_vars = json.load(f)['Oracle']
#     dsn_tns = cx_Oracle.makedsn(host_name, port,service_name)
#     return cx_Oracle.connect(user_name, password, dsn_tns)


def create_sql_alchemy_connection():
    host_name, port, service_name, user_name, password = read_oracle_connection_variables()
    con_string = "oracle+cx_oracle://{0}:{1}@{2}:{3}/{4}?encoding=UTF-8".format(user_name, password, host_name, str(port), service_name)
    e = create_engine(con_string)
    return e.connect()


def create_data_frame(query):
    try:
        con = create_sql_alchemy_connection()
        frame = create_data_frame_with_sql_con(query, con)
    except ValueError as e:
        print(e)
    finally:
        close_connection(con)
        return frame


def close_connection(con):
    con.close();


def test_connection(query):
    print(create_data_frame(query))


if __name__=="__main__":
    test_connection("select * from actor_5")
