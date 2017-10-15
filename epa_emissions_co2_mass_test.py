#import database
import pandas as pd
from datetime import datetime
import numpy as np

states = {"CA", "TX", "FL", "NY"}

GET_SOURCE_NAME = 'EPA_EMISSIONS_CO2_MASS_STATE_MONTH'
SOURCE_NAME = 'EPA_EMISSIONS_CO2_MASS_TEST'
SOURCE_DESCR = '{meta: "STATE", ref: "STATE", date: "MONTH", value: "CO2_MASS (kilotons) Test"}'

def get_source_last_update(database):
    return database.get_last_update(SOURCE_NAME, None)

def run(database):
	print('ccc')
	all = pd.DataFrame([])
	for state in states:
		df = database.get_data(GET_SOURCE_NAME, state)
		if not df.empty:
			print(state)
			df['date'] = pd.to_datetime(df['date'])
			df = df.set_index('date')
			df = df.groupby(pd.TimeGrouper(freq='2M'))['value'].sum().reset_index()
			df['ref'] = state
			df['meta'] = state
			df['value'] = df['value'] / 1000
			df = df.round(2)
			df['file_date'] = datetime.now()
			all = pd.concat([all, df])
	if not all.empty:
		all = all[['ref', 'date', 'value', 'meta', 'file_date']]
		database.put_all_data(SOURCE_NAME, SOURCE_DESCR, all)

#run()