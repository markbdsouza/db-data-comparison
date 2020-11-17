from sqlalchemy import create_engine
from Config.ReadConfig import read_oracle_connection_variables
from DataframeLogic.GenericDFActivities import create_data_frame_with_sql_con


def create_sql_alchemy_connection():
    """Create and return a connection to the Oracle Database given the details in config.json"""
    host_name, port, service_name, user_name, password = read_oracle_connection_variables()
    con_string = "oracle+cx_oracle://{0}:{1}@{2}:{3}/{4}?encoding=UTF-8".format(user_name, password, host_name, str(port), service_name)
    e = create_engine(con_string)
    return e.connect()


def create_data_frame(query):
    """Given the Oracle query, create the dataframe and return it after closing the connection"""
    try:
        con = create_sql_alchemy_connection()
        frame = create_data_frame_with_sql_con(query, con)
    except ValueError as e:
        print(e)
    except UnboundLocalError as e:
        print(e)
    finally:
        close_connection(con)
        return frame


def close_connection(con):
    """Close the connection once complete"""
    con.close();


def test_connection():
    """To test the DB Connection. Replace the query with a table in the DB and run the file to test it"""
    print(create_data_frame("select * from actor_5"))


if __name__=="__main__":
    test_connection()
