from CONSTANTS import EXCEPTION_ORACLE_DATA_FRAME_EMPTY, EXCEPTION_MY_SQL_DATA_FRAME_EMPTY, \
    SOURCE, TARGET
from Connections.AllConnections import create_data_frames
from Connections.ReadConfig import read_db_details, read_input_queries_list
from DataframeLogic import DataProfiling, comparison
from DataframeLogic.GenericDFActivities import rename_df_columns, drop_df_columns


def test_name_swap(df1, df2):
    print('Performing Name swap')
    rename_df_columns(df2, {'first_name_2': 'first_name', 'last_name_2': 'last_name'})
    drop_df_columns(df2, ['last_name'])
    return df1, df2


def fetch_db_details():
    source, target = read_db_details()
    query_list = read_input_queries_list()
    for item in query_list:
        source_query = item['SourceQuery']
        target_query = item['TargetQuery']
        test_set_name = item['TestSet']
        fetch_data_frame(source, source_query, target, target_query, test_set_name)


def fetch_data_frame(source, source_query, target, target_query, test_set_name):
    df1, df2, source_summary_data_frame, target_summary_data_frame = create_data_frames(source, source_query, target,target_query)
    if df1 is None:
        print(EXCEPTION_ORACLE_DATA_FRAME_EMPTY)
    elif df2 is None:
        print(EXCEPTION_MY_SQL_DATA_FRAME_EMPTY)
    else:
        run_validation_and_profiling(df1, df2, source_summary_data_frame, target_summary_data_frame, test_set_name)


def run_validation_and_profiling(df1, df2, source_summary_data_frame, target_summary_data_frame, test_set_name):
    try:
        df1, df2 = test_name_swap(df1, df2)
        comparison.count_validation(df1, df2, test_set_name)
        comparison.data_validation(df1, df2)
        DataProfiling.create_data_profiling(df1, SOURCE, source_summary_data_frame)
        DataProfiling.create_data_profiling(df2, TARGET, target_summary_data_frame)
    except KeyError as e:
        print('Key Error: {}'.format(e))
    except ValueError as e:
        print('Value Error: {}'.format(e))

if __name__ == "__main__":
    fetch_db_details()
