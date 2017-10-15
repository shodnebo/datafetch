from flask import Flask, render_template, request
from datetime import date
import csv_helper as database
import epa_emissions_co2_mass
import epa_emissions_co2_mass_state_month
import epa_emissions_co2_mass_test
import gviz_api
import pandas as pd

app = Flask(__name__)

def doupdate(source_name):
    if source_name == 'EPA_EMISSIONS_CO2_MASS':
        epa_emissions_co2_mass.run(database)
    if source_name == 'EPA_EMISSIONS_CO2_MASS_STATE_MONTH':
        epa_emissions_co2_mass_state_month.run(database)
    if source_name == 'EPA_EMISSIONS_CO2_MASS_TEST':
        epa_emissions_co2_mass_test.run(database)
    return source_name

@app.route('/', methods=['GET', 'POST'])
def homepage():
    if request.method == 'POST':
        source_name = request.form['source']
        print(source_name)
        doupdate(source_name)
    lastUpdate1 = database.get_last_update('EPA_EMISSIONS_CO2_MASS', None)
    lastUpdate2 = database.get_last_update('EPA_EMISSIONS_CO2_MASS_STATE_MONTH', None)
    lastUpdate3 = database.get_last_update('EPA_EMISSIONS_CO2_MASS_TEST', None)
    status1 = database.get_status('EPA_EMISSIONS_CO2_MASS')
    status2 = database.get_status('EPA_EMISSIONS_CO2_MASS_STATE_MONTH')
    status3 = database.get_status('EPA_EMISSIONS_CO2_MASS_TEST')
    return render_template('template.html', status1=status1, status2=status2, status3=status3
                           , lastUpdate1=lastUpdate1, lastUpdate2=lastUpdate2, lastUpdate3=lastUpdate3
                          , json_chart=getchart('EPA_EMISSIONS_CO2_MASS_STATE_MONTH'), json_table=gettable('EPA_EMISSIONS_CO2_MASS_STATE_MONTH'))

@app.route('/update')
def update():
    source_name = request.args.get('source')
    if source_name == 'EPA_EMISSIONS_CO2_MASS':
        print(source_name)
        epa_emissions_co2_mass.run(database)
    if source_name == 'EPA_EMISSIONS_CO2_MASS_STATE_MONTH':
        epa_emissions_co2_mass_state_month.run(database)
    return source_name

def getchart(source_name):
    df = database.get_data(source_name, '')
    df['date'] = pd.to_datetime(df['date'])
    df = df.pivot_table(index=['date'], columns='ref', values='value').reset_index()
    gpa_desc = []
    gpa_desc.append(("date", "date", "Date"))
    for col in df.columns:
        if col is not "date":
            gpa_desc.append((col, "number", col))
    gpa_data_table = gviz_api.DataTable(gpa_desc)
    gpa_values = df.values
    gpa_data_table.LoadData(gpa_values)
    gpa_json = gpa_data_table.ToJSon()
    return gpa_json

def gettable(source_name):
    df = database.get_data(source_name, '')
    df['value'] = df['value'] / 1000
    df['date'] = pd.to_datetime(df['date'])
    df = df[(df['date'] > date(2017, 1, 1))]
    df = df.pivot_table(index=['date'], columns='ref', values='value').reset_index()
    df = df[['date', 'CA', 'FL', 'TX', 'NY']]
    gpa_desc = []
    gpa_desc.append(("date", "date", "Date"))
    for col in df.columns:
        if col is not "date":
            gpa_desc.append((col, "number", col))
    data_table = gviz_api.DataTable(gpa_desc)
    data_table.LoadData(df.values)
    json_table = data_table.ToJSon()
    return json_table

if __name__ == '__main__':
    app.run(debug=True, use_reloader=True)
