from zipfile import ZipFile
import io
import urllib.request
import pandas as pd
import database
from datetime import datetime
from datetime import date
import helpers

FETCH_ALL = False
START_DATE = datetime(2017, 1, 1)
SOURCE_NAME = 'EPA_EMISSIONS_CO2_MASS'
SOURCE_DESCR = '{meta: "STATE;FACILITY_NAME", ref: "ORISPL_CODE", date: "OP_DATE", value: "CO2_MASS (tons)"}'

def run():
    df = pd.DataFrame()
    last_curve_update = database.get_last_update(SOURCE_NAME, START_DATE) #Get the
    print(last_curve_update)
    currentYear = date.today().year
    files1 = helpers.get_ftp_files('ftp.epa.gov', '/dmdnload/emissions/daily/quarterly/'+str(currentYear-1), START_DATE)
    files2 = helpers.get_ftp_files('ftp.epa.gov', '/dmdnload/emissions/daily/quarterly/'+str(currentYear), START_DATE)
    files = files1.copy()
    files.update(files2)
    for file in files:
        print(file)
        mysock = urllib.request.urlopen(file)  # check urllib for parameters
        last_modified = mysock.headers['last-modified']
        if last_modified is not None:
            last_modified_date = datetime.strptime(mysock.headers['last-modified'], '%a, %d %b %Y %H:%M:%S GMT')
        else:
            last_modified_date = files[file]
        if last_modified_date <= last_curve_update and not FETCH_ALL:
            continue;
        print(file+'  '+str(last_modified_date)+'  '+str(last_curve_update))
        memfile = io.BytesIO(mysock.read())
        with ZipFile(memfile, 'r') as myzip:
            for name in myzip.namelist():
                print('Procssing '+name)
                new_df = pd.read_csv(myzip.open(name))
                new_df['file_date'] = last_modified_date
                # Fetch CSV and append to DataFrame
                df = pd.concat([df, new_df])
    if not df.empty:
        df = df.dropna(subset=['CO2_MASS (tons)'], how='all')       # Remove all entries with null CO2_MASS
        df['ref'] = df['ORISPL_CODE']                               # Use only facility as key. Rename to unique curve ref.
        df['meta'] = df['STATE']+";"+df['FACILITY_NAME']            # Store selected curve details as meta
        df.rename(columns={'OP_DATE': 'date', 'CO2_MASS (tons)': 'value'}, inplace=True)  # Rename to expected curve columns date and value
        df = df[['ref', 'date', 'value', 'meta', 'file_date']]      # Restrict DataFrame to values expected by database.put_all_data
        df = df.groupby(['ref', 'date', 'file_date', 'meta'], as_index=False).sum()  # Aggregate on facility and date
        database.put_all_data(SOURCE_NAME, SOURCE_DESCR, df)        # Store data by source name

run()