from Config.CONSTANTS import INPUT_FILE_PATH_STRING
from Config.ReadConfig import read_csv_connection_variables, read_txt_connection_variables
from DataframeLogic.GenericDFActivities import create_data_frame_from_csv, create_data_frame_from_txt


def fetch_csv_data_frame():
    """Create and return the CSV data frame"""
    file_name = read_csv_connection_variables()
    file_path = INPUT_FILE_PATH_STRING.format(file_name)
    return create_data_frame_from_csv(file_path)


def  fetch_txt_data_frame():
    """Create and return the TXT data frame given the delimiter"""
    file_name, delimiter = read_txt_connection_variables()
    file_path = INPUT_FILE_PATH_STRING.format(file_name)
    return create_data_frame_from_txt(file_path, delimiter)
