import DataProfiling
import Connections.AllConnections


# https://stackoverflow.com/questions/17095101/compare-two-dataframes-and-output-their-differences-side-by-side

def get_data_frame_count(df):
    return len(df.index)


def data_frame_difference(df1, df2, which=None, is_to_be_sorted=False, sort_column_list=['actor_id'],
                          is_sorted_ascending=True):
    """Find rows which are different between two DataFrames."""
    # print (df1.dtypes)
    comparison_df = df1.merge(df2,
                              indicator=True,
                              how='outer'
                              # , on=['actor_id']
                              )
    if which is None:
        diff_df = comparison_df[comparison_df['_merge'] != 'both']
    else:
        diff_df = comparison_df[comparison_df['_merge'] == which]
    diff_df.to_csv('diff.csv')
    if is_to_be_sorted:
        diff_df = sort_data_frame(diff_df, sort_column_list, is_sorted_ascending)
    return diff_df


def sort_data_frame(df, column_name_list, is_sorted_ascending=True):
    return df.sort_values(by=column_name_list, ascending=is_sorted_ascending)


def rename_df_columns(df, column_rename_dict={'first_name_2': 'first_name', 'last_name_2': 'last_name'}):
    return df.rename(columns=column_rename_dict, inplace=True)


def drop_df_columns(df, list_of_columns=['colheading1', 'colheading2']):
    try:
        df.drop(list_of_columns, axis=1, inplace=True)
    except KeyError:
        print('Drop column failed. Column not found in the dataframe.')


def count_validation(df1, df2):
    print('COUNT VALIDATION')
    source_count = get_data_frame_count(df1)
    target_count = get_data_frame_count(df2)
    if source_count != target_count:
        print('Failed. Source has :{} rows. Target has :{} rows'.format(source_count, target_count))
    else:
        print('Passed')


def test_name_swap(df1, df2):
    print('Performing Name swap')
    rename_df_columns(df1, {'first_name_2': 'first_name', 'last_name_2': 'last_name'})
    # drop_df_columns(df1, ['last_name'])
    return df1, df2


def print_column_names(df):
    print(df.dtypes)


def data_validation(df1, df2):
    df1,df2 = test_name_swap(df1, df2)
    print(df1)
    print('DATA VALIDATION')
    diff = data_frame_difference(df1, df2)
    if diff.empty:
        print('Validation Passed')
    else:
        print('Validation Failed')
        print(diff)


if __name__ == "__main__":
    df1, df2, source_summary_data_frame, target_summary_data_frame = Connections.AllConnections.create_data_frames()
    if df1 is None:
        print('No Object found in Oracle')
    elif df2 is None:
        print('No Object found in SQL')
    else:
        try:
            count_validation(df1, df2)
            data_validation(df1, df2)
            DataProfiling.create_data_profiling(df1, "Source", source_summary_data_frame)
            DataProfiling.create_data_profiling(df2, "Target", target_summary_data_frame)
        except KeyError as e:
            print('Key Error: {}'.format(e))
        except ValueError as e:
            print('Value Error: {}'.format(e))
