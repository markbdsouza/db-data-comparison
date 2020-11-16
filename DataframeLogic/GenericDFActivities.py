import pandas as pd

from CONSTANTS import QUERY


def rename_df_columns(df, column_rename_dict={'first_name_2': 'first_name', 'last_name_2': 'last_name'}):
    return df.rename(columns=column_rename_dict, inplace=True)


def drop_df_columns(df, list_of_columns=['colheading1', 'colheading2']):
    try:
        df.drop(list_of_columns, axis=1, inplace=True)
    except KeyError:
        print('Drop column failed. Column not found in the dataframe.')


def get_data_frame_count(df):
    return len(df.index)


def sort_data_frame(df, column_name_list, is_sorted_ascending=True):
    return df.sort_values(by=column_name_list, ascending=is_sorted_ascending)


def create_summary_data_frame(type, db, query):
    return pd.DataFrame([[type, db], [QUERY, query],[]])


def print_column_names(df):
    print(df.dtypes)


def create_data_frame_with_sql_con(query,con):
    return pd.read_sql(query, con)