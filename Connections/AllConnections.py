import json
import pandas as pd
from Connections.MySQLConnection import create_data_frame as create_my_sql_data_frame
from Connections.OracleConnection import create_data_frame as create_oracle_data_frame


def call_db_frame_create(type, query):
    if type == 'Oracle':
        return create_oracle_data_frame(query)
    if type == 'MySQL':
        return create_my_sql_data_frame(query)


def create_data_frames():
    with open('./config.json') as f:
        config = json.load(f)
        source = config['Source']
        source_query = config['SourceQuery']
        target = config['Target']
        target_query = config['TargetQuery']
    df1 = call_db_frame_create(source, source_query)
    df2 = call_db_frame_create(target, target_query)
    return df1, df2, create_summary_data_frame('source', source, source_query), create_summary_data_frame('target', target, target_query)


def create_summary_data_frame(type, db, query):
    return pd.DataFrame([[type, db], ['query', query],[]])
