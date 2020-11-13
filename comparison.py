import MySQLConnection
import OracleConnection

# https://stackoverflow.com/questions/17095101/compare-two-dataframes-and-output-their-differences-side-by-side


def data_frame_difference(df1, df2, which=None):
    """Find rows which are different between two DataFrames."""
    # print (df1.dtypes)
    comparison_df = df1.merge(df2,
                              indicator=True,
                              how='outer', on=['first_name'])
    if which is None:
        diff_df = comparison_df[comparison_df['_merge'] != 'both']
    else:
        diff_df = comparison_df[comparison_df['_merge'] == which]
    diff_df.to_csv('diff.csv')
    return diff_df


def rename_df_columns(df, column_rename_dict={'first_name_2': 'first_name', 'last_name_2': 'last_name'}):
    return df.rename(columns=column_rename_dict, inplace=True)


def drop_df_columns(df, list_of_columns= ['colheading1', 'colheading2']):
    df.drop(list_of_columns, axis=1, inplace=True)

df1 = OracleConnection.create_data_frame()
df2 = MySQLConnection.create_data_frame()

if (df1 is None or df2 is None):
    print('No Object found')
else:
    print(df1.dtypes)
    rename_df_columns(df1, {'first_name_2': 'first_name', 'last_name_2': 'last_name'})
    drop_df_columns(df1, ['last_name'])
    print(df1.dtypes)
    # print(df1[df1.middle_name.notnull()])
    print(data_frame_difference(df2, df1))
    # print(data_frame_difference(df1, df2))

