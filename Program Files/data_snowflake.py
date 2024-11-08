import snowflake.connector
import os

# Establish the connection
conn = snowflake.connector.connect(
    user='your_username',
    password='your_password',
    account='your_account',
    warehouse='your_warehouse',
    database='your_database',
    schema='your_schema',
)
cursor= conn.cursor()

#creating a storage integration
create_storage_integration=""" 
		CREATE STORAGE INTEGRATION IF NOT EXISTS MY_INTEGRATION
		TYPE=EXTERNAL_STAGE
		STORAGE_PROVIDER=S3
		ENABLED=TRUE
		STORAGE_AWS_ROLE_ARN='arn:aws:iam::597088026789:role/snowflake-role'
		STORAGE_ALLOWED_LOCATIONS=('s3://s3-bucket-for-data/'); 
	"""
cursor.execute(create_storage_integration)

#creating a file format
create_file_format=""" 
		CREATE FILE FORMAT IF NOT EXISTS MY_FORMAT
		TYPE = 'CSV' 
		FIELD_DELIMITER = ',';
 """
cursor.execute(create_file_format)

matches=['ODI_RECORDS','TEST_RECORDS','T20_RECORDS','IPL_RECORDS']

#creating tables
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

#creating stages
for stage in matches:
	create_stmt_for_stage=f""" 
			CREATE STAGE IF NOT EXISTS {stage}_stage
			URL='s3://s3-bucket-for-data/{stage}/'
			STORAGE_INTEGRATION=MY_INTEGRATION
			COMMENT='This is my external storage for aws';
	 """
	cursor.execute(create_stmt_for_stage)

create_stmt_for_players_stage=f""" 
			CREATE STAGE IF NOT EXISTS players_information_stage
			URL='s3://s3-bucket-for-data/players_information_records/'
			STORAGE_INTEGRATION=MY_INTEGRATION
			COMMENT='This is my external storage for aws';
	 """
cursor.execute(create_stmt_for_stage)


#creating pipes
for pipe in matches:
	create_pipe=f""" 
		CREATE PIPE IF NOT EXISTS {pipe}_pipe
		AUTO_INGEST=TRUE AS
			COPY INTO {pipe}
			FROM @{pipe}_stage
			FILE_FORMAT = MY_FORMAT; 
	"""
	cursor.execute(create_pipe)

create_pipe_for_players=f""" 
		CREATE PIPE IF NOT EXISTS players_pipe
		AUTO_INGEST=TRUE AS
			COPY INTO PLAYERS_INFORMATION_RECORDS
			FROM @players_information_stage
			FILE_FORMAT = MY_FORMAT; 
	"""
cursor.execute(create_pipe_for_players)
conn.commit()

# this function imports all files into snowflake internal stage
def importing_files(team_name):
	replaced_team_name=team_name.replace(' ', '_')
	if os.path.exists({your_path}/{replaced_team_name}'):
		if ' ' in team_name:
			import_statement=fr"""
				PUT file:{your_path}/{replaced_team_name}/*.csv @{your_internal_stage_name}/{replaced_team_name}/
			"""
			cursor.execute(import_statement)
			conn.commit()
		else:
			import_statement=f"""
				PUT file:{your_path}/{replaced_team_name}/*.csv @{your_internal_stage_name}/{replaced_team_name}/
			"""
