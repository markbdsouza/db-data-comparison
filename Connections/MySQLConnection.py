from sqlalchemy import create_engine,exc
from Config.ReadConfig import read_mysql_config_variables
from DataframeLogic.GenericDFActivities import create_data_frame_with_sql_con


def create_my_sql_connection():
    """Fetch details from config.json and return MY SQL Connection to the DB"""
    host_name, user_name, password, schema = read_mysql_config_variables()
    sqlEngine = create_engine('mysql+pymysql://{}:{}@{}/{}'.format(user_name, password, host_name,schema), pool_recycle=3600)
    return sqlEngine.connect()


def create_data_frame(query):
    """Create the DB Connection and run the query to create the data frame and return once created"""
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
    """Close connection once complete"""
    con.close();


def test_connection():
    """To test the DB Connection. Replace the query with a table in the DB and run the file to test it"""
    print(create_data_frame("select * from actor"))


if __name__=="__main__":
    test_connection()
