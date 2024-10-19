import psycopg2
import time
import os

#creating a connection
start_time = time.time()
conn = psycopg2.connect(dbname="your_database_name", user="your_username", password="your_password", host="your_host")
cursor = conn.cursor()

#sql statement to insert the players information data
player_info_statement= """ 
            INSERT INTO PLAYERS_INFORMATION_RECORDS(NAME, AGE, ROLE, TEAM)
            VALUES (%s, %s, %s, %s) ON CONFLICT (NAME) DO NOTHING;
    """

#sql statement to insert the odi records data
odi_statement= """ 
            INSERT INTO ODI_RECORDS(NAME, BATTING_MATCHES, TOTAL_RUNS, FIFTIES, HUNDREDS, BATTING_AVERAGE, BATTING_STRIKE_RATE, BOWLED_MATCHES, BOWLED_OVERS, WICKETS_TAKEN, BOWLING_AVERAGE, BOWLING_STRIKE_RATE, BOWLING_ECONOMY, TEAM) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (NAME) DO UPDATE SET
            NAME=EXCLUDED.NAME, BATTING_MATCHES=EXCLUDED.BATTING_MATCHES, TOTAL_RUNS=EXCLUDED.TOTAL_RUNS, FIFTIES=EXCLUDED.FIFTIES, HUNDREDS=EXCLUDED.HUNDREDS, BATTING_AVERAGE=EXCLUDED.BATTING_AVERAGE, BATTING_STRIKE_RATE=EXCLUDED.BATTING_STRIKE_RATE, BOWLED_MATCHES=EXCLUDED.BOWLED_MATCHES, BOWLED_OVERS=EXCLUDED.BOWLED_OVERS, WICKETS_TAKEN=EXCLUDED.WICKETS_TAKEN, BOWLING_AVERAGE=EXCLUDED.BOWLING_AVERAGE, BOWLING_STRIKE_RATE=EXCLUDED.BOWLING_STRIKE_RATE, BOWLING_ECONOMY=EXCLUDED.BOWLING_ECONOMY, TEAM=EXCLUDED.TEAM;        
    """

#sql statement to insert the test records data
test_statement= """ 
            INSERT INTO TEST_RECORDS(NAME, BATTING_MATCHES, TOTAL_RUNS, FIFTIES, HUNDREDS, BATTING_AVERAGE, BATTING_STRIKE_RATE, BOWLED_MATCHES, BOWLED_OVERS, WICKETS_TAKEN, BOWLING_AVERAGE, BOWLING_STRIKE_RATE, BOWLING_ECONOMY, TEAM) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (NAME) DO UPDATE SET
            NAME=EXCLUDED.NAME, BATTING_MATCHES=EXCLUDED.BATTING_MATCHES, TOTAL_RUNS=EXCLUDED.TOTAL_RUNS, FIFTIES=EXCLUDED.FIFTIES, HUNDREDS=EXCLUDED.HUNDREDS, BATTING_AVERAGE=EXCLUDED.BATTING_AVERAGE, BATTING_STRIKE_RATE=EXCLUDED.BATTING_STRIKE_RATE, BOWLED_MATCHES=EXCLUDED.BOWLED_MATCHES, BOWLED_OVERS=EXCLUDED.BOWLED_OVERS, WICKETS_TAKEN=EXCLUDED.WICKETS_TAKEN, BOWLING_AVERAGE=EXCLUDED.BOWLING_AVERAGE, BOWLING_STRIKE_RATE=EXCLUDED.BOWLING_STRIKE_RATE, BOWLING_ECONOMY=EXCLUDED.BOWLING_ECONOMY, TEAM=EXCLUDED.TEAM;
 """

#sql statement to insert the t20 records data
t20_statement= """ 
            INSERT INTO T20_RECORDS(NAME, BATTING_MATCHES, TOTAL_RUNS, FIFTIES, HUNDREDS, BATTING_AVERAGE, BATTING_STRIKE_RATE, BOWLED_MATCHES, BOWLED_OVERS, WICKETS_TAKEN, BOWLING_AVERAGE, BOWLING_STRIKE_RATE, BOWLING_ECONOMY, TEAM) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (NAME) DO UPDATE SET
            NAME=EXCLUDED.NAME, BATTING_MATCHES=EXCLUDED.BATTING_MATCHES, TOTAL_RUNS=EXCLUDED.TOTAL_RUNS, FIFTIES=EXCLUDED.FIFTIES, HUNDREDS=EXCLUDED.HUNDREDS, BATTING_AVERAGE=EXCLUDED.BATTING_AVERAGE, BATTING_STRIKE_RATE=EXCLUDED.BATTING_STRIKE_RATE, BOWLED_MATCHES=EXCLUDED.BOWLED_MATCHES, BOWLED_OVERS=EXCLUDED.BOWLED_OVERS, WICKETS_TAKEN=EXCLUDED.WICKETS_TAKEN, BOWLING_AVERAGE=EXCLUDED.BOWLING_AVERAGE, BOWLING_STRIKE_RATE=EXCLUDED.BOWLING_STRIKE_RATE, BOWLING_ECONOMY=EXCLUDED.BOWLING_ECONOMY, TEAM=EXCLUDED.TEAM;
 """

#sql statement to insert the ipl records data
ipl_statement= """ 
            INSERT INTO IPL_RECORDS(NAME, BATTING_MATCHES, TOTAL_RUNS, FIFTIES, HUNDREDS, BATTING_AVERAGE, BATTING_STRIKE_RATE, BOWLED_MATCHES, BOWLED_OVERS, WICKETS_TAKEN, BOWLING_AVERAGE, BOWLING_STRIKE_RATE, BOWLING_ECONOMY, TEAM) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (NAME) DO UPDATE SET
            NAME=EXCLUDED.NAME, BATTING_MATCHES=EXCLUDED.BATTING_MATCHES, TOTAL_RUNS=EXCLUDED.TOTAL_RUNS, FIFTIES=EXCLUDED.FIFTIES, HUNDREDS=EXCLUDED.HUNDREDS, BATTING_AVERAGE=EXCLUDED.BATTING_AVERAGE, BATTING_STRIKE_RATE=EXCLUDED.BATTING_STRIKE_RATE, BOWLED_MATCHES=EXCLUDED.BOWLED_MATCHES, BOWLED_OVERS=EXCLUDED.BOWLED_OVERS, WICKETS_TAKEN=EXCLUDED.WICKETS_TAKEN, BOWLING_AVERAGE=EXCLUDED.BOWLING_AVERAGE, BOWLING_STRIKE_RATE=EXCLUDED.BOWLING_STRIKE_RATE, BOWLING_ECONOMY=EXCLUDED.BOWLING_ECONOMY, TEAM=EXCLUDED.TEAM;
 """

#receives structured data and changing into tuple and calling the insert function to insert the data into respective tables
def insert_into_odi(data):
    tuples = [tuple(x) for x in data.to_numpy()]
    for records in tuples:
        cursor.execute(odi_statement, records)
        conn.commit()
def insert_into_test(data):
    tuples = [tuple(x) for x in data.to_numpy()]
    for records in tuples:
        cursor.execute(test_statement, records)
        conn.commit()
def insert_into_t20(data):
    tuples = [tuple(x) for x in data.to_numpy()]
    for records in tuples:
        cursor.execute(t20_statement, records)
        conn.commit()
def insert_into_ipl(data):
    tuples = [tuple(x) for x in data.to_numpy()]
    for records in tuples:
        cursor.execute(ipl_statement, records)
        conn.commit()
def insert_into_players_info(data):
    tuples = [tuple(x) for x in data.to_numpy()]
    for records in tuples:
        cursor.execute(player_info_statement, records)
        conn.commit()

def update(matches):
    match_type=['odi','test','t20','ipl']
    for idx,match in enumerate(matches):
        update_statement=f""" 
                    UPDATE PLAYERS_INFORMATION_RECORDS
                    SET {match_type[idx]}_ID={match}.ID
                    FROM {match}
                    WHERE PLAYERS_INFORMATION_RECORDS.NAME={match}.NAME;
        """
        cursor.execute(update_statement)
        conn.commit()

def export(team_name):
    tables=['players_information_records', 'odi_records','test_records','t20_records','ipl_records']
    replaced_team_name=team_name.replace(' ', '_')
    for table in tables:
        folder_path={your_path}
        select_query=f"select * from {table} where team ilike ('{team_name}')"
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            with open(f'{folder_path}/{table}.csv', 'w') as f:
                cursor.copy_expert(f"COPY ({select_query}) TO STDOUT WITH CSV DELIMITER ','", f)
        else:
            with open(f'{folder_path}/{table}.csv', 'w') as f:
                cursor.copy_expert(f"COPY ({select_query}) TO STDOUT WITH CSV DELIMITER ','", f)
