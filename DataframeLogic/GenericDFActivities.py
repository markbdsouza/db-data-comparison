import pandas as pd
from Config.CONSTANTS import QUERY, EXCEPTION_DROP_COLUMN_KEY_ERROR


def rename_df_columns(df, column_rename_dict):
    """Rename column in a data frame
    Sample value column_rename_dict = {'first_name_2': 'first_name', 'last_name_2': 'last_name'} """
    return df.rename(columns=column_rename_dict, inplace=True)


def drop_df_columns(df, list_of_columns):
    """Drop data frame columns
    Sample value list_of_columns=['colheading1', 'colheading2']"""
    try:
        df.drop(list_of_columns, axis=1, inplace=True)
    except KeyError as e:
        print(EXCEPTION_DROP_COLUMN_KEY_ERROR.format(e))


def get_data_frame_count(df):
    """Get number of rows of a data frame"""
    return len(df.index)


def sort_data_frame(df, column_name_list, is_sorted_ascending=True):
    """sort the data frame based on the list of columns passed"""
    return df.sort_values(by=column_name_list, ascending=is_sorted_ascending)


def convert_column_names_to_lower(df):
    df.columns = [c.lower() for c in df.columns]
    return df

def create_summary_data_frame(type, db, query):
    """create a summary data frame for a source/target"""
    return pd.DataFrame([[type, db], [QUERY, query],[]])


def create_summary_data_frame_without_query(type, db):
    """create a summary data frame for a source/target without a query"""
    return pd.DataFrame([[type, db], []])


def fetch_column_names(df):
    """return the types of the columns in a data frame """
    return df.dtypes


def create_data_frame_with_sql_con(query,con):
    """return the data frame when passed a connection object and a query"""
    return convert_column_names_to_lower(pd.read_sql(query, con))


def create_data_frame_from_csv(path):
    """return the data frame from a CSV File when passed the file path"""
    return convert_column_names_to_lower(pd.read_csv(path))


def create_data_frame_from_txt(path, sep = ","):
    """return the data frame from a txt File when passed the file path and delimiter"""
    return convert_column_names_to_lower(pd.read_csv(path, sep = sep ))


def create_empty_df():
    """return an empty data frame"""
    return pd.DataFrame()