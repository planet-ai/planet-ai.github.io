import requests
import pandas as pd
from matplotlib import pyplot as plt
from scipy.stats import gamma
import numpy as np
import csv

"""
Output Column Index

"FIPS": 0,
"ID": 1,
"SOCIAL_DIST_INDEX": 2,
"INCOME": 3,
"POPU_DENSITY": 4,
"DAYS_SINCE_1ST_CASE_X": 5,
"NUMBER_OF_BEDS": 6,
"AVG_WEATHER": 7,
"AIRPORT_SIZE": 8,
"INFECTED_X": 9,
"RECOVERED_X": 10,
"DEAD_X": 11,
"INFECTED_Y": 12,
"RECOVERED_Y": 13,
"DEAD_Y": 14,
"GROCERY_AVG_VISITS": 15,
"HEALTHCARE_AVG_VISITS": 16,
"HOSPITAL_AVG_VISITS": 17,
"POI_AVG_VISITS": 18,
"TOTAL_POP": 19,
"POP_DENSITY": 20,
"CLIMATE": 21
"""

class Data():
	def __init__(self, fips, x_date, y_date):
		self.fips = fips
		self.x_date = x_date
		self.y_date = y_date
		self.county_info = self.get_county_info()
		self.population_info = self.get_population_info()
		self.climate_info = self.get_climate_info()
		self.data = []
		self.infected_ts_data = []
		self.death_ts_data = []

	def get_ts_data(self, from_date, to_date = pd.Timestamp.now().strftime("%Y-%m-%d")):
		headers = ["FIPS", "COUNTY_ID"]

		for k in self.county_info.keys():
			temp_infected_record = []
			temp_death_record = []
			print("Geting data for " + k + "...")
			try:
				self.fips = self.county_info[k]
				medical_population = self.get_medical_population()
				county_id = medical_population['id'].values[0] # ID

				temp_infected_record.append(self.fips)
				temp_infected_record.append(county_id)
				temp_death_record.append(self.fips)
				temp_death_record.append(county_id)

				# ------------ Initial Searching Time
				cases = self.get_cases(county_id, from_date, to_date)

				for case in cases.values:
					if len(headers) < (2 + len(cases)):
						headers.append(case[0].strftime("%Y-%m-%d"))
					temp_infected_record.append(case[1])
					temp_death_record.append(case[2])

				if len(self.infected_ts_data) == 0 or len(self.death_ts_data) == 0:
					self.infected_ts_data.append(headers)
					self.death_ts_data.append(headers)
				self.infected_ts_data.append(temp_infected_record)
				self.death_ts_data.append(temp_death_record)
				print(self.infected_ts_data)
				print(self.death_ts_data)
				print("Appended!")
			except:
				print("No Data for this county")

	def get_data(self):
		headers = [
			"FIPS", "ID", "SOCIAL_DIST_INDEX", "INCOME", "POPU_DENSITY", 
			"DAYS_SINCE_1ST_CASE_X","NUMBER_OF_BEDS", "AVG_WEATHER", 
			"AIRPORT_SIZE", "INFECTED_X", "RECOVERED_X", "DEAD_X", 
			"INFECTED_Y", "RECOVERED_Y", "DEAD_Y", "GROCERY_AVG_VISITS",
			"HEALTHCARE_AVG_VISITS", "HOSPITAL_AVG_VISITS", "POI_AVG_VISITS", 
			"TOTAL_POP", "POP_DENSITY", "CLIMATE"
		]

		self.data.append(headers)

		grocery_avg_visits = self.get_visits("./open_source/grocery_visits.csv")
		healthcare_avg_visits = self.get_visits("./open_source/healthcare_visits.csv")
		hospital_avg_visits = self.get_visits("./open_source/hospital_visits.csv")
		poi_avg_visits = self.get_visits("./open_source/poi_visits.csv")

		for k in self.county_info.keys():
			print("Geting data for " + k + "...")
			try:
				temp = ["NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", 
				"NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA"]

				self.fips = self.county_info[k]
				temp[0] = self.county_info[k] # FIPS
				pop_data = self.population_info[k]
				temp[19] = pop_data[0]
				temp[20] = pop_data[1]
				temp[21] = self.climate_info[k]
				temp[15] = grocery_avg_visits[self.fips]
				temp[16] = healthcare_avg_visits[self.fips]
				temp[17] = hospital_avg_visits[self.fips]
				temp[18] = poi_avg_visits[self.fips]

				medical_population = self.get_medical_population()
				temp[6] = int(medical_population['hospitalStaffedBeds'])
				temp[1] = medical_population['id'].values[0] # ID

				# ------------ Initial Searching Time
				from_date = "2020-01-01"

				today = pd.Timestamp.now().strftime("%Y-%m-%d")

				cases = self.get_cases(temp[1], from_date, today)

				days_since_1st_cases = 0

				for case in cases.values:
					if case[1] > 0 and case[0].strftime("%Y-%m-%d") <= self.x_date:
						days_since_1st_cases += 1

					if case[0].strftime("%Y-%m-%d") == self.x_date:
						temp[9] = case[1]
						temp[11] =  case[2]

					if case[0].strftime("%Y-%m-%d") == self.y_date:
						temp[12] = case[1]
						temp[14] =  case[2]

				temp[5] = days_since_1st_cases

				self.data.append(temp)
				print(self.data[-1])
				print("Appended!")
			except:
				print("No Data for this county")

	def get_test_data(self):
		for k in self.county_info.keys():
			try:
				temp = ["NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", 
				"NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA"]
				self.fips = self.county_info[k]
				medical_population = self.get_medical_population()
				temp[1] = medical_population['id'].values[0]
				from_date = "2020-01-01"

				today = pd.Timestamp.now().strftime("%Y-%m-%d")

				cases = self.get_cases(temp[1], from_date, today)

				days_since_1st_cases = 0

				for case in cases.values:
					if case[1] > 0:
						days_since_1st_cases += 1

					if case[0].strftime("%Y-%m-%d") == self.search_date:
						temp[12] = case[1]
						temp[14] =  case[2]

				temp[5] = days_since_1st_cases
				temp[9] = cases.values[-1][1]
				temp[11] = cases.values[-1][2]

				print(temp)
			except:
				print()

	def get_county_info(self):
		data = ""
		county_info = {}

		with open('./open_source/county_fips.csv', newline='') as f:
			reader = csv.reader(f)
			data = list(reader)

		data.pop(0)

		for i in data:
			county_info[i[2] + "-" + i[1]] = i[0]

		return county_info

	def get_population_info(self):
		data = ""
		population_info = {}

		with open('./open_source/pop_stats.csv', newline='') as f:
			reader = csv.reader(f)
			data = list(reader)

		data.pop(0)

		for i in data:
			population_info[i[0] + "-" + i[1]] = [i[2], i[3]]

		return population_info

	def save_to_csv(self, data, output_path):
		with open(output_path, 'w', newline='') as f:
			for i in data:
				string_to_write = ",".join(map(str, i)) + "\n"
				f.write(string_to_write)

	def get_cases(self, county_id, from_date, to_date):
		daily_cases_body = {
			"spec" : {
				"filter" : "fips == \'" + self.fips + "\'",
				"expressions" : ["JHU_ConfirmedCases", "JHU_ConfirmedDeaths", "JHU_ConfirmedRecoveries"],
				"start" : from_date,
				"end" : to_date,
				"interval" : "DAY",
			}
		}

		raw_data = self.evalmetrics("outbreaklocation", daily_cases_body)
		data = raw_data[['dates', county_id + '.JHU_ConfirmedCases.data', county_id + '.JHU_ConfirmedDeaths.data']]
		return data

	def get_medical_population(self):
		raw_data = self.fetch("outbreaklocation", {"spec" : {"filter" : "fips == \'" + self.fips + "\'"}})
		data = raw_data[['hospitalIcuBeds', 'hospitalStaffedBeds', 'hospitalLicensedBeds', 'latestTotalPopulation', 'id']]

		return data

	def get_visits(self, file_path):
		data = ""
		visits = {}

		with open(file_path, newline='') as f:
			reader = csv.reader(f)
			data = list(reader)

		data.pop(0)

		for i in data:
			visit_list = list(map(float, i[1:len(i)]))
			visits[str(int(i[0]))] = sum(visit_list) / len(visit_list)

		return visits

	def get_climate_info(self):
		data = ""
		climate_info = {}

		with open('./open_source/climate_data.csv', newline='') as f:
			reader = csv.reader(f)
			data = list(reader)

		data.pop(0)

		for i in data:
			climate_info[i[0] + "-" + i[1]] = i[2]

		return climate_info

	def get_daily_new_cases(self, fips, from_date, to_date = pd.Timestamp.now().strftime("%Y-%m-%d")):
		medical_raw_data = self.fetch("outbreaklocation", {"spec" : {"filter" : "fips == \'" + fips + "\'"}})
		medical_population = medical_raw_data[['hospitalIcuBeds', 'hospitalStaffedBeds', 'hospitalLicensedBeds', 'latestTotalPopulation', 'id']]
		county_id = medical_population['id'].values[0]

		from_date = "2020-01-01"
		new_cases = {}
		daily_cases_body = {
			"spec" : {
				"filter" : "fips == \'" + self.fips + "\'",
				"expressions" : ["JHU_ConfirmedCases", "JHU_ConfirmedDeaths", "JHU_ConfirmedRecoveries"],
				"start" : from_date,
				"end" : to_date,
				"interval" : "DAY",
			}
		}

		cases_raw_data = self.evalmetrics("outbreaklocation", daily_cases_body)
		data = cases_raw_data[['dates', county_id + '.JHU_ConfirmedCases.data', county_id + '.JHU_ConfirmedDeaths.data']]

		prev_infected = 0
		prev_death = 0

		for case in data.values:
			print(case)
			new_cases[case[0].strftime("%Y-%m-%d")] = [(case[1] - prev_infected), (case[2] - prev_death)]
			prev_infected = case[1] # update infected accumulated cases
			prev_death = case[2] # update death accumulated cases

		return new_cases

	def get_daily_new_cases_counties(self):
		# Grab FIPS data
		data = ""
		fips_info = {}

		with open('./open_source/climate_data.csv', newline='') as f:
			reader = csv.reader(f)
			data = list(reader)

		data.pop(0)

		for i in data:
			fips_info[i[2] + "-" + i[1]] = i[0]

		# search the cases
		data = ""
		infected_info = []
		death_info = []

		# make sure to manually adjust the from_date and to_date in csv file
		with open('./open_source/infections_timeseries.csv', newline='') as f:
			reader = csv.reader(f)
			data = list(reader)

		infected_info.append(data[0])
		data.pop(0)
		for i in data:
			temp = [i[0], i[1], 0]
			for j in range(3, len(i)):
				temp.append(int(i[j]) - int(i[j-1]))
			infected_info.append(temp)

		# make sure to manually adjust the from_date and to_date in csv file
		with open('./open_source/deaths_timeseries.csv', newline='') as f:
			reader = csv.reader(f)
			data = list(reader)

		death_info.append(data[0])
		data.pop(0)
		for i in data:
			temp = [i[0], i[1], 0]
			for j in range(3, len(i)):
				temp.append(int(i[j]) - int(i[j-1]))
			death_info.append(temp)

		return infected_info, death_info


	# #################################################################################
	# C3.ai COVID-19 API Documentation (2.0): https://c3.ai/covid-19-api-documentation/
	# #################################################################################
	def read_data_json(self, typename, api, body):
	    """
	    read_data_json directly accesses the C3.ai COVID-19 Data Lake APIs using the requests library, 
	    and returns the response as a JSON, raising an error if the call fails for any reason.
	    ------
	    typename: The type you want to access, i.e. 'OutbreakLocation', 'LineListRecord', 'BiblioEntry', etc.
	    api: The API you want to access, either 'fetch' or 'evalmetrics'.
	    body: The spec you want to pass. For examples, see the API documentation.
	    """
	    response = requests.post(
	        "https://api.c3.ai/covid/api/1/" + typename + "/" + api, 
	        json = body, 
	        headers = {
	            'Accept' : 'application/json', 
	            'Content-Type' : 'application/json'
	        }
	    )
	    response.raise_for_status()
	    
	    return response.json()

	def fetch(self, typename, body, get_all = False, remove_meta = True):
	    """
	    fetch accesses the Data Lake using read_data_json, and converts the response into a Pandas dataframe. 
	    fetch is used for all non-timeseries data in the Data Lake, and will call read_data as many times 
	    as required to access all of the relevant data for a given typename and body.
	    ------
	    typename: The type you want to access, i.e. 'OutbreakLocation', 'LineListRecord', 'BiblioEntry', etc.
	    body: The spec you want to pass. For examples, see the API documentation.
	    get_all: If True, get all records and ignore any limit argument passed in the body. If False, use the limit argument passed in the body. The default is False.
	    remove_meta: If True, remove metadata about each record. If False, include it. The default is True.
	    """
	    if get_all:
	        has_more = True
	        offset = 0
	        limit = 2000
	        df = pd.DataFrame()

	        while has_more:
	            body['spec'].update(limit = limit, offset = offset)
	            response_json = self.read_data_json(typename, 'fetch', body)
	            new_df = pd.json_normalize(response_json['objs'])
	            df = df.append(new_df)
	            has_more = response_json['hasMore']
	            offset += limit
	            
	    else:
	        response_json = self.read_data_json(typename, 'fetch', body)
	        df = pd.json_normalize(response_json['objs'])
	        
	    if remove_meta:
	        df = df.drop(columns = [c for c in df.columns if ('meta' in c) | ('version' in c)])
	    
	    return df
	    
	def evalmetrics(self, typename, body, get_all = False, remove_meta = True):
	    """
	    evalmetrics accesses the Data Lake using read_data_json, and converts the response into a Pandas dataframe.
	    evalmetrics is used for all timeseries data in the Data Lake.
	    ------
	    typename: The type you want to access, i.e. 'OutbreakLocation', 'LineListRecord', 'BiblioEntry', etc.
	    body: The spec you want to pass. For examples, see the API documentation.
	    get_all: If True, get all metrics and ignore limits on number of expressions and ids. If False, consider expressions and ids limits. The default is False.
	    remove_meta: If True, remove metadata about each record. If False, include it. The default is True.
	    """
	    if get_all:
	        expressions = body['spec']['expressions']
	        ids = body['spec']['ids']
	        df = pd.DataFrame()
	        
	        for ids_start in range(0, len(ids), 10):
	            for expressions_start in range(0, len(expressions), 4):
	                body['spec'].update(
	                    ids = ids[ids_start : ids_start + 10],
	                    expressions = expressions[expressions_start : expressions_start + 4]
	                )
	                response_json = self.read_data_json(typename, 'evalmetrics', body)
	                new_df = pd.json_normalize(response_json['result'])
	                new_df = new_df.apply(pd.Series.explode)
	                df = pd.concat([df, new_df], axis = 1)
	            
	    else:
	        response_json = self.read_data_json(typename, 'evalmetrics', body)
	        df = pd.json_normalize(response_json['result'])
	        df = df.apply(pd.Series.explode)

	    # get the useful data out
	    if remove_meta:
	        df = df.filter(regex = 'dates|data|missing')
	    
	    # only keep one date column
	    date_cols = [col for col in df.columns if 'dates' in col]
	    keep_cols =  date_cols[:1] + [col for col in df.columns if 'dates' not in col]
	    df = df.filter(items = keep_cols).rename(columns = {date_cols[0] : "dates"})
	    df["dates"] = pd.to_datetime(df["dates"])
	    
	    return df
	# #################################################################################
	
if __name__ == '__main__':
	fips = "39061"
	x_date = "2020-05-01"
	y_date = "2020-05-15"

	# --------------------------------------- 
	data_api = Data(fips, x_date, y_date)
	
	# --------------------------- 1
	#data_api.get_data()
	#data_api.save_to_csv(data_api.data, './hyper_output.csv')

	# --------------------------- 2
	#data_api.get_ts_data("2020-01-01")
	#data_api.save_to_csv(data_api.infected_ts_data, './infected_ts_output.csv')
	#data_api.save_to_csv(data_api.death_ts_data, './death_ts_output.csv')

	# --------------------------- 3
	infected_daily, death_daily = data_api.get_daily_new_cases_counties()
	data_api.save_to_csv(infected_daily, './output/infected_daily_county_output.csv')
	data_api.save_to_csv(death_daily, './output/death_daily_county_output.csv')










