import database
import pandas as pd
from datetime import datetime

states = {"AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
          "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD",
          "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"}

GET_SOURCE_NAME = 'EPA_EMISSIONS_CO2_MASS'
SOURCE_NAME = 'EPA_EMISSIONS_CO2_MASS_STATE_MONTH'
SOURCE_DESCR = '{meta: "STATE", ref: "STATE", date: "MONTH", value: "CO2_MASS (tons) Montly Sum"}'

def run():
	all = pd.DataFrame([])
	for state in states:
		df = database.get_data(GET_SOURCE_NAME, state + ';')
		if not df.empty:
			print(state)
			df['date'] = pd.to_datetime(df['date'])
			df = df.set_index('date')
			df = df.groupby(pd.TimeGrouper(freq='M'))['value'].sum().reset_index()
			df['ref'] = state
			df['meta'] = state
			df['file_date'] = datetime.now()
			all = pd.concat([all, df])

	all = all[['ref', 'date', 'value', 'meta', 'file_date']]
	database.put_all_data(SOURCE_NAME, SOURCE_DESCR, all)

run()