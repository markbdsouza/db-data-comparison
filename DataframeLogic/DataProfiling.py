import pandas as pd
from CONSTANTS import DATA_PROFILING_OUTPUT_FILE_NAME, DATA_PROFILING_HEADER_NAMES, DATA_PROFILING_PRINT_MSG
from OutputCSVFile import append_data_frame_to_csv_with_header, write_data_frame_to_csv_without_header


def create_max_min_value_series(df, index_list):
    max_list = []
    min_list = []
    for column in df:
        max_list.append(df[column].dropna().max())
        min_list.append(df[column].dropna().min())
    max_values = pd.Series(max_list, index=index_list)
    min_values = pd.Series(min_list, index=index_list)
    return max_values, min_values


def create_data_profiling(df, profiling_file_name, summary_df):
    print(DATA_PROFILING_PRINT_MSG)
    unique = df.nunique()
    is_na = df.isna().sum()
    index_list = list(unique.index)
    max_values, min_values = create_max_min_value_series(df, index_list)
    mean_value = df.mean()
    merged_df = pd.concat([unique, is_na, max_values, min_values, mean_value], axis=1)
    merged_df.columns = DATA_PROFILING_HEADER_NAMES
    save_results(merged_df, profiling_file_name, summary_df)
    print(merged_df)


def save_results(df, profiling_name, summary_df):
    file_name = DATA_PROFILING_OUTPUT_FILE_NAME.format(profiling_name)
    write_data_frame_to_csv_without_header(summary_df, file_name)
    append_data_frame_to_csv_with_header(df, file_name)

