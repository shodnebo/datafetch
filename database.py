import psycopg2
import pandas as pd
import csv_helper
import os
from datetime import datetime

use_csv = False;

def get_connection():
    host = 'db'
    if os.environ['OS'] == 'Windows_NT':
        host = 'localhost'
    return psycopg2.connect(host=host, user='postgres', password='admin', database='demo');

def get_csv(source):
    try:
        df = pd.read_csv('data/' + source + '.csv')
    except (OSError, IOError) as e:
        df = pd.DataFrame()
        print(e)
    return df;

def get_tables():
    cnx = get_connection()
    query = "SELECT table_name FROM information_schema.tables where table_schema=%(schema)s";
    df = pd.read_sql(query, params={"schema": "demo"}, con=cnx)
    cnx.close()
    return df

def get_data(source_name, meta_filter):
    if use_csv:
        return csv_helper.get_data(source_name, meta_filter)

    cnx = get_connection()
    query = "select meta, c.curve_id as ref, value_date as date, value from curve_data d, curve c, source s " \
            "where c.curve_id = d.curve_id and s.source_id = c.source_id " \
            "and s.source_name = %(source_name)s and c.meta like %(meta_filter)s"
    df = pd.read_sql(query, params={"source_name": source_name, "meta_filter": meta_filter+'%'}, con=cnx)
    cnx.close()
    return df

def put_all_data(source_name, descr, df):
    if use_csv:
        return csv_helper.put_all_data(source_name, descr, df)

    df = df.groupby('ref').agg({'date': lambda x: x.tolist(), 'value': lambda x: x.tolist(), 'meta': lambda x: max(x),
                                'file_date': lambda x: max(x)})
    cnx = get_connection()
    try:
        for key, row in df.iterrows():
            cursor = cnx.cursor()
            curve_values_s = ','.join(str(e) for e in row['value'])
            curve_date_s = ','.join(e for e in row['date'])
            print(source_name+ ' '+str(key))
            file_date = row['file_date']
            meta = row['meta']
            put_data = ("SELECT * FROM put_source_data(%s, %s, %s, %s, %s, %s, %s);")
            #cur.execute("SELECT * FROM stored_procedure_name( %s,%s); ", (value1, value2))
            data = (source_name, descr, meta, file_date, str(key), curve_date_s, curve_values_s)
            cursor.execute(put_data, data)
            cursor.close()
        cnx.commit()
    finally:
        cnx.close()

    log_message('put_all_data '+source_name+' '+str(len(df.index)))

def get_status(source_name):
    cnx = get_connection()
    func = "select status from source where source_name = %s"
    cursor = cnx.cursor()
    cursor.execute(func, [source_name])
    row = cursor.fetchone()
    cursor.close()
    cnx.close()
    if row is None:
        return 'EMPTY';
    else:
        return row[0]

def set_status(source_name, status):
    cnx = get_connection()
    func = "update source set status = %s where source_name = %s"
    cursor = cnx.cursor()
    cursor.execute(func, (status, source_name))
    cursor.close()
    cnx.commit()
    cnx.close()

def log_message(log_message):
    cnx = get_connection()
    func = "insert into debug_log(log_date, log_message) values(%s, %s)"
    cursor = cnx.cursor()
    cursor.execute(func, (datetime.now(), log_message))
    cursor.close()
    cnx.commit()
    cnx.close()

def get_last_update(source_name, alternative_date):
    if use_csv:
        return csv_helper.get_last_update(source_name, alternative_date)
    cnx = get_connection()
    cursor = cnx.cursor()
    func = "select get_last_update(%s)"
    cursor.execute(func, [source_name])
    row = cursor.fetchone()
    cursor.close()
    cnx.close()
    if row is None:
        return alternative_date;
    else:
        return row[0] or alternative_date
