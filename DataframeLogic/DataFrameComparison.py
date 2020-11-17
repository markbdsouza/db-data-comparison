from Config.CONSTANTS import DATA_COMPARISON_PRINT_MSG, VALIDATION_FAILED, VALIDATION_SUCCESS, COUNT_VALIDATION_SUCCESS, \
    COUNT_VALIDATION_FAILURE, COUNT_VALIDATION_PRINT_MSG, DATA_COMPARISON_VALIDATION_FILE_NAME, SOURCE_COUNT, \
    TARGET_COUNT, MISMATCH_COUNT, NAME, KEY_ERROR, VALUE_ERROR
from Connections.AllConnections import create_data_frames
from DataframeLogic.GenericDFActivities import get_data_frame_count, sort_data_frame, create_empty_df
from Config.OutputFileConfig import write_list_to_csv, append_data_frame_to_csv_with_header, append_list_to_csv


def data_frame_difference(df1, df2, which=None, is_to_be_sorted=False, sort_column_list =[],
                          is_sorted_ascending=True):
    """Find rows which are different between two DataFrames"""
    comparison_df = df1.merge(df2, indicator=True, how='outer')
    if which is None:
        diff_df = comparison_df[comparison_df['_merge'] != 'both']
    else:
        diff_df = comparison_df[comparison_df['_merge'] == which]
    if is_to_be_sorted:
        diff_df = sort_data_frame(diff_df, sort_column_list, is_sorted_ascending)
    return diff_df


def count_validation(df1, df2, test_set_name):
    """Perform Count Validation and update output file"""
    print(COUNT_VALIDATION_PRINT_MSG)
    source_count = get_data_frame_count(df1)
    target_count = get_data_frame_count(df2)
    if source_count != target_count:
        print(COUNT_VALIDATION_FAILURE.format(source_count, target_count))
        result = VALIDATION_FAILED
    else:
        print(COUNT_VALIDATION_SUCCESS.format(source_count))
        result = VALIDATION_SUCCESS
    list_to_write = [[NAME, test_set_name], [COUNT_VALIDATION_PRINT_MSG, result], [SOURCE_COUNT, source_count],
                     [TARGET_COUNT, target_count]]
    write_list_to_csv(DATA_COMPARISON_VALIDATION_FILE_NAME.format(test_set_name), list_to_write)


def data_validation(df1, df2, test_set_name):
    """Call data frame difference method, handle exceptions and save values to output csv file"""
    diff_df = create_empty_df()
    try:
        print(DATA_COMPARISON_PRINT_MSG)
        diff_df = data_frame_difference(df1, df2)
        if diff_df.empty:
            print(VALIDATION_SUCCESS)
            list_to_write = [[], [DATA_COMPARISON_PRINT_MSG, VALIDATION_SUCCESS]]
        else:
            print(VALIDATION_FAILED)
            print(diff_df)
            list_to_write = [[], [DATA_COMPARISON_PRINT_MSG, VALIDATION_FAILED],
                             [MISMATCH_COUNT, get_data_frame_count(diff_df)]]
    except KeyError as e:
        print(KEY_ERROR.format(e))
        list_to_write = [[], [DATA_COMPARISON_PRINT_MSG, KEY_ERROR.format(e)]]
    except ValueError as e:

        print(VALUE_ERROR.format(e))
        list_to_write = [[], [DATA_COMPARISON_PRINT_MSG, VALUE_ERROR.format(e)]]
    finally:
        append_list_to_csv(DATA_COMPARISON_VALIDATION_FILE_NAME.format(test_set_name), list_to_write)
        if not diff_df.empty:
            append_data_frame_to_csv_with_header(diff_df, DATA_COMPARISON_VALIDATION_FILE_NAME.format(test_set_name))


if __name__ == "__main__":
    df1, df2, source_summary_data_frame, target_summary_data_frame = create_data_frames()
