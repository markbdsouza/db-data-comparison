from Config.CONSTANTS import SOURCE, TARGET
from Connections.FileConnection import fetch_csv_data_frame, fetch_txt_data_frame
from Connections.MySQLConnection import create_data_frame as create_my_sql_data_frame
from Connections.OracleConnection import create_data_frame as create_oracle_data_frame
from DataframeLogic.GenericDFActivities import create_summary_data_frame, create_summary_data_frame_without_query


def call_db_frame_create(db_type, query):
    """Depending on type of database, call the respective data frame creation method"""
    if db_type == 'Oracle':
        return create_oracle_data_frame(query)
    if db_type == 'MySQL':
        return create_my_sql_data_frame(query)
    if db_type == 'CSV':
        return fetch_csv_data_frame()
    if db_type == 'TXT':
        return fetch_txt_data_frame()


def fetch_summary_data_frame(source, source_value, query):
    """Depending on type of database, Create the summary data frame"""
    if source_value in('Oracle','MySQL'):
        return create_summary_data_frame(source, source_value, query)
    if source_value in ('CSV','TXT'):
        return create_summary_data_frame_without_query(source, source_value)


def create_data_frames(source, source_query, target,target_query):
    """Given source and target details and query, return the object dataframe and the summary dataframe for source
    and target """
    df1 = call_db_frame_create(source, source_query)
    df2 = call_db_frame_create(target, target_query)
    return df1, df2, fetch_summary_data_frame(SOURCE, source, source_query), fetch_summary_data_frame(TARGET, target, target_query)


if __name__=="__main__":
    create_data_frames()

