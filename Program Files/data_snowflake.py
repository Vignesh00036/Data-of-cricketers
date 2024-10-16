import snowflake.connector

# Establish the connection
conn = snowflake.connector.connect(
    user='your_username',
    password='your_password',
    account='your_account',
    warehouse='your_warehouse_name',
    database='your_database_name',
    schema='your_schema_name',
)
matches=['ODI_RECORDS','TEST_RECORDS','T20_RECORDS','IPL_RECORDS']
cursor=conn.cursor()

for match in matches:
	create_statement_for_records = f""" 
	CREATE TABLE IF NOT EXISTS {match}(
	    ID INT UNIQUE AUTOINCREMENT,
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
		TEAM VARCHAR,
		PRIMARY KEY (ID)
		);
	"""
	cursor.execute(create_statement_for_records)

create_statement_for_players_info=""" 
        CREATE TABLE IF NOT EXISTS PLAYERS_INFORMATION_RECORDS(
            ID int unique autoincrement,
            NAME VARCHAR UNIQUE,
            AGE INT,ROLE VARCHAR,
            TEAM VARCHAR,
            ODI_ID INT UNIQUE REFERENCES ODI_RECORDS(ID),
            TEST_ID INT UNIQUE REFERENCES TEST_RECORDS(ID),
            T20_ID INT UNIQUE REFERENCES T20_RECORDS(ID),
            IPL_ID INT UNIQUE REFERENCES IPL_RECORDS(ID), 
            PRIMARY KEY (id));
	"""

cursor.execute(create_statement_for_players_info)
conn.commit()


def importing_files(team_name):
	import_statement=f"""
		PUT file:{path_to_your_files} @{path_to_your_destination}
	"""
	cursor.execute(import_statement)
	conn.commit()
