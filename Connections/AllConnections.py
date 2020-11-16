from CONSTANTS import SOURCE, TARGET
from Connections.MySQLConnection import create_data_frame as create_my_sql_data_frame
from Connections.OracleConnection import create_data_frame as create_oracle_data_frame
from Connections.ReadConfig import read_db_details
from DataframeLogic.GenericDFActivities import create_summary_data_frame


def call_db_frame_create(type, query):
    if type == 'Oracle':
        return create_oracle_data_frame(query)
    if type == 'MySQL':
        return create_my_sql_data_frame(query)


def create_data_frames(source, source_query, target,target_query):

    df1 = call_db_frame_create(source, source_query)
    df2 = call_db_frame_create(target, target_query)
    return df1, df2, create_summary_data_frame(SOURCE, source, source_query), create_summary_data_frame(TARGET, target, target_query)


if __name__=="__main__":
    create_data_frames()

