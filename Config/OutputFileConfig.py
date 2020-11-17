import csv


def write_data_frame_to_csv(df, file_name, index=False, header=True, mode='w'):
    df.to_csv(file_name, index=index, header=header, mode=mode)


def append_data_frame_to_csv_with_header(df, file_name):
    df.to_csv(file_name, header=True, mode='a', index=False)


def write_data_frame_to_csv_without_header(df, file_name):
    df.to_csv(file_name, header=False, mode='w', index=False)


def write_list_to_csv(file_name, list_of_lists_of_data):
    with open(file_name, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(list_of_lists_of_data)


def append_list_to_csv(file_name, list_of_lists_of_data):
    with open(file_name, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(list_of_lists_of_data)
