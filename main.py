from Config.CONSTANTS import EXCEPTION_ORACLE_DATA_FRAME_EMPTY, EXCEPTION_MY_SQL_DATA_FRAME_EMPTY, \
    SOURCE, TARGET
from Connections.AllConnections import create_data_frames
from Config.ReadConfig import read_input_queries_list
from DataframeLogic import DataFrameDataProfiling, DataFrameComparison


def fetch_db_details():
    query_list = read_input_queries_list()
    for item in query_list:
        source = item['Source']
        source_query = item['SourceQuery']
        target = item['Target']
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
    DataFrameComparison.count_validation(df1, df2, test_set_name)
    DataFrameComparison.data_validation(df1, df2, test_set_name)
    DataFrameDataProfiling.create_data_profiling(df1, SOURCE, source_summary_data_frame, test_set_name)
    DataFrameDataProfiling.create_data_profiling(df2, TARGET, target_summary_data_frame, test_set_name)


if __name__ == "__main__":
    fetch_db_details()
