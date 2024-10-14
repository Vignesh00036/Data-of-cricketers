from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import psycopg2
import time
import data_collector
import data_snowflake
import data_aws_s3

start_time = time.time()
conn = psycopg2.connect(dbname="cricketers", user="postgres", password="@Beast00036@", host="localhost")
cursor = conn.cursor()
matches=['ODI_RECORDS','TEST_RECORDS','T20_RECORDS','IPL_RECORDS']

team_number = 1
page_number = 5
teams=[]
team_numbers = [
	1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 19, 20, 21,
	22, 25, 26, 28, 29, 117, 151, 552, 637, 750, 1123, 1188, 1298, 1379,
	1481, 1565, 1671, 1720, 1723, 1730, 1731, 1733, 1734, 1735, 1739, 1745,
	1747, 1751, 1753, 1757, 1761, 1763, 1765, 1767, 1771, 1774, 1779, 1785,
	1789, 1791, 1793, 1795, 1797, 1802, 1805, 1807, 1811, 1818, 1820, 1822,
	1826, 1828, 1835, 1857, 1863, 1865, 1868, 1870, 1872, 2833
]

def return_teams(team):
	for idx, team_present in enumerate(teams):
		if int(team)==idx:
			return team_present,team_numbers[idx]
		elif team==team_present:
			return team_present,team_numbers[idx]
					
def teams_data(driver):
	for i in range(146):
		team_present=driver.find_element(By.XPATH, f'//*[@id="team{i}"]').get_attribute('value').lower()
		if 'women' not in team_present.lower() and 'under-19' not in team_present.lower():
			splitting=driver.find_element(By.XPATH, f'//*[@id="team{i}"]').get_attribute('value').lower().split(' (')
			splitted=splitting[0]
			teams.append(splitted[0].upper()+splitted[1:])

for match in matches:
	create_statement_for_records = f""" 
		CREATE TABLE IF NOT EXISTS {match}(
		ID SERIAL PRIMARY KEY UNIQUE,
		NAME VARCHAR UNIQUE,
		BATTING_MATCHES INT,
		FIFTIES INT,
		HUNDREDS INT, 
		TOTAL_RUNS INT, 
		BATTING_AVERAGE FLOAT, 
		BATTING_STRIKE_RATE FLOAT, 
		BOWLED_MATCHES INT, 
		BOWLED_OVERS FLOAT, 
		WICKETS_TAKEN INT, 
		BOWLING_AVERAGE FLOAT, 
		BOWLING_STRIKE_RATE FLOAT, 
		BOWLING_ECONOMY FLOAT,
		TEAM VARCHAR
		)
	"""
	cursor.execute(create_statement_for_records)

create_statement_for_players_info=""" 
			CREATE TABLE IF NOT EXISTS PLAYERS_INFORMATION_RECORDS(
			ID SERIAL PRIMARY KEY,
			NAME VARCHAR UNIQUE,
			AGE INT,
			ROLE VARCHAR,
			TEAM VARCHAR,
			ODI_ID INT UNIQUE REFERENCES ODI_RECORDS(ID),
			TEST_ID INT UNIQUE REFERENCES TEST_RECORDS(ID),
			T20_ID INT UNIQUE REFERENCES T20_RECORDS(ID),
			IPL_ID INT UNIQUE REFERENCES IPL_RECORDS(ID)
			) 
"""
cursor.execute(create_statement_for_players_info)

conn.commit()

chrome_options = Options()
chrome_options.add_argument("--headless")
service = Service(r'/media/beast/Beast/DE/Python_programms/1_OG/chromedriver')
driver = webdriver.Chrome(service=service, options=chrome_options)
driver.get(f'https://www.hindustantimes.com/cricket/players/page-{page_number}?team={team_number}')
teams_data(driver)
teams.append('All')
for idx,data in enumerate(teams):
	print(idx, data)
how_many_teams=int(input('How many teams are you going to scrap the data?\n'))
while how_many_teams:
	need_to_iterate=[]
	if how_many_teams==0:
		print('Please enter a valid number')
	else:
		for i in range(how_many_teams):
			team_to_insert=input('Which team(s) do you want to scrap the data, type index or name of the team or type All to scrap all team? ')
			if team_to_insert=='All' or team_to_insert=='ALL' or team_to_insert=='79':
				for idx,team in enumerate(team_numbers):
					print(f'Scarapping {teams[idx]} data')
					data_collector.navigator(driver, page_number, team, teams[idx], matches)
					data_snowflake.importing_files(teams[idx])
					data_aws_s3.upload(teams[idx])
				break
			elif 'from' in team_to_insert or 'From' in team_to_insert:
				starting_team=team_to_insert.split(' ')
				for j in range(int(starting_team[1]), len(team_numbers)):
					print(f'Scarapping {teams[int(starting_team[1])]} data')
					data_collector.navigator(driver, page_number, team_numbers[int(starting_team[1])], teams[int(starting_team[1])], matches)
					data_snowflake.importing_files(teams[int(starting_team[1])])
					data_aws_s3.upload(teams[int(starting_team[1])])
			elif int(team_to_insert)>=0 and int(team_to_insert)<79:
				need_to_iterate.append(return_teams(team_to_insert))
			else:
				print('Please enter a valid number!!!')
		for k in need_to_iterate:
			print(f'Scarapping {k[0]} data')
			data_collector.navigator(driver, page_number, k[1], k[0], matches)
			data_snowflake.importing_files(k[0])
			data_aws_s3.upload(k[0])
		break

taken_time = time.time()-start_time

if taken_time>3600:
	remaining_seconds=taken_time%3600
	minutes=remaining_seconds//60
	print(f'Took {int(taken_time//3600)}.{int(minutes)} hrs')
elif taken_time>60:
    print(f'Took {int(taken_time//60)}.{int(taken_time%60)} mins')
elif taken_time<1:
    print(f'Took {round(taken_time/60,4)} m/s')
else:
    print(f'Took {round(taken_time,2)} s')