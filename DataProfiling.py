import pandas as pd


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
    print('DATA PROFILING')
    unique = df.nunique()
    is_na = df.isna().sum()
    index_list = list(unique.index)
    max_values, min_values = create_max_min_value_series(df, index_list)
    mean_value = df.mean()
    merged_df = pd.concat([unique, is_na, max_values, min_values, mean_value], axis=1)
    merged_df.columns = ['Unique Count', 'Null Count', 'Maximum Value', 'Minimum Value', 'Mean Value']
    save_results(merged_df, profiling_file_name, summary_df)
    print(merged_df)


def save_results(df, profiling_name, summary_df):
    file_name = '{}_profiling_results.csv'.format(profiling_name)
    summary_df.to_csv(file_name, index=False, header=False)
    df.to_csv(file_name, mode='a', header=True)
