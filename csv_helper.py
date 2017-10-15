import pandas as pd
from datetime import datetime
import os

def get_csv(source):
    try:
        df = pd.read_csv('data/' + source + '.csv')
    except (OSError, IOError) as e:
        df = pd.DataFrame()
        print(e)
    return df;

def get_status(source_name):
    return '';

def set_status(source_name, status):
    return;

def get_data(source_name, meta_filter):
    df = get_csv(source_name)
    df = df[df['meta'].str.contains(meta_filter)]
    return df

def put_all_data(source_name, descr, df):
    local = get_csv(source_name)
    result = pd.concat([local, df]).drop_duplicates(['ref', 'date'])
    result = result.sort_values(by=['ref', 'date'])
    if not os.path.exists('data'):
        os.makedirs('data')
    result.to_csv('data/'+source_name+'.csv', columns=['ref', 'date', 'meta', 'value', 'file_date'], quoting=1, index=False)
    df['file_date'] = pd.to_datetime(df['file_date'])
    date = df['file_date'].max()
    date = date.today().replace(microsecond=0)
    lu = pd.DataFrame(data=[[source_name, date, 'None']], columns=['Name', 'Date', 'Status'])
    try:
        lu_new = pd.read_csv('data/last-update.csv')
    except (OSError, IOError) as e:
        lu_new = lu
    result = pd.concat([lu, lu_new]).drop_duplicates(['Name'])
    result.to_csv('data/last-update.csv', quoting=1, index=False)
    print(result)

def get_last_update(source_name, alternative_date):
    try:
        df = pd.read_csv('data/last-update.csv', index_col='Name')
    except (OSError, IOError) as e:
        return None
    if df.empty or source_name not in df.index:
        return alternative_date;
    date = df.get_value(source_name, "Date", takeable=False)
    return datetime.strptime(date, '%Y-%m-%d %H:%M:%S')