import MySQLdb
import os
import pandas as pd
from datetime import date, datetime, timedelta

use_csv = True;

def get_csv(source):
    try:
        df = pd.read_csv('data/' + source + '.csv')
    except (OSError, IOError) as e:
        df = pd.DataFrame()
        print(e)
    return df;

def get_data(source_name, meta_filter):
    if use_csv:
        df = get_csv(source_name)
        df = df[df['meta'].str.contains(meta_filter)]
        return df

    cnx = MySQLdb.connect(user='demo', password='demo', database='demo')
    cursor = cnx.cursor()
    cursor.close()
    cnx.close()

def put_all_data(source_name, descr, df):
    if use_csv:
        local = get_csv(source_name)
        result = pd.concat([local, df]).drop_duplicates(['ref', 'date'])
        result = result.sort_values(by=['ref', 'date'])
        if not os.path.exists('data'):
            os.makedirs('data')
        result.to_csv('data/'+source_name+'.csv', columns=['ref', 'date', 'meta', 'value', 'file_date'], quoting=1, index=False)
        return;

    df = df.groupby('ref').agg({'date': lambda x: x.tolist(), 'value': lambda x: x.tolist(), 'meta': lambda x: max(x),
                                'file_date': lambda x: max(x)})
    cnx = MySQLdb.connect(user='root', password='admin', database='demo')
    for key, row in df.iterrows():
        cursor = cnx.cursor()
        curve_values_s = ','.join(str(e) for e in row['value'])
        curve_date_s = ','.join(str(e) for e in row['date'])
        print(source_name+ ' '+str(key))
        file_date = row['file_date']
        meta = row['meta']
        put_data = ("call demo.put_source_data(%s, %s, %s, %s, %s, %s, %s);")
        data = (source_name, descr, meta, file_date, str(key), curve_date_s, curve_values_s)
        cursor.execute(put_data, data)
        cursor.close()
    cnx.commit()
    cnx.close()

def get_last_update(source_name, alternative_date):
    if use_csv:
        df = get_csv(source_name)
        if df.empty:
            return alternative_date;
        date = get_csv(source_name)['file_date'].max()
        return datetime.strptime(date, '%Y-%m-%d %H:%M:%S')

    cnx = MySQLdb.connect(user='root', password='admin', database='demo')
    cursor = cnx.cursor()
    func = "SELECT demo.GET_LAST_UPDATE(%s)"
    cursor.execute(func, [source_name])
    row = cursor.fetchone()
    return row[0]
