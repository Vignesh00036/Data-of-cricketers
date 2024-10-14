import requests
from bs4 import BeautifulSoup
import pandas as pd
from selenium.webdriver.common.by import By
import data_psql


# Variables
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
name = []
age = []
role = []
teams = []
table = []
match_types = []


#this function is going to get all players information
def data_collector1(page_number, team_number, team_name):
    response = requests.get(f'https://www.hindustantimes.com/cricket/players/page-{page_number}?team={team_number}', headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    for i in soup.find_all('div', class_='plrsList'):
        for names in i.find_all('span', class_='name'):
            name.append(names.text)
        for ages in i.find_all('span', class_='age'):
            if (' | ' in ages.text):
                splitted = ages.text.split(' | ')
                role.append(splitted[0])
                age.append(splitted[1].replace('Age: ', ''))
            else :
                role.append(ages.text)
                age.append(0)
    for k in name:
        teams.append(team_name)
    info=pd.DataFrame({'Name':name,'Age':age,'Role':role,'Team':teams})
    data_psql.insert_into_players_info(info)


#this function is going to call the other function to insert the structured data
def insert_data(data, match_type_name):
    if match_type_name=='odi':
        data_psql.insert_into_odi(data)
    elif match_type_name=='test':
        data_psql.insert_into_test(data)
    elif match_type_name=='t20':
        data_psql.insert_into_t20(data)
    else:
        data_psql.insert_into_ipl(data)


#this is going to form the data structured and going to send the structured data to insert_data function
def records(datas, name, table, match_type_name):
    pd.set_option('future.no_silent_downcasting', True)
    if len(datas)==3:
        overall_stats = {'Name':[name],'Matches':[table[0]],'Runs':[table[1]],'50s':[table[2]],'100s':[table[3]],'bat_average':[table[4]],'bat_s/r':[table[5]],'bowl_matches':[table[6]],'bowl_overs':[table[7]],'Wickets':[table[8]],'bowl_average':[table[9]],'bowl_s/r':[table[10]],'bowl_economy':[table[11]], 'team':teams[0]}
        data = pd.DataFrame(overall_stats)
        data.replace(to_replace=r'-', value=0, inplace=True)
        data.replace(to_replace='', value=0, inplace=True)
        insert_data(data, match_type_name.text.lower())
    elif len(datas)==2:
        if f'{name} Batting Records' in datas and f'{name} Fielding Records' in datas:
            overall_stats = {'Name':[name],'Matches':[table[0]],'Runs':[table[1]],'50s':[table[2]],'100s':[table[3]],'bat_average':[table[4]],'bat_s/r':[table[5]],'bowl_matches':[table[0]],'bowl_overs':0,'Wickets':0,'bowl_average':0,'bowl_s/r':0,'bowl_economy':0, 'team':teams[0]}
            data = pd.DataFrame(overall_stats)
            data.replace(to_replace=r'-', value=0, inplace=True)
            data.replace(to_replace='', value=0, inplace=True)
            insert_data(data, match_type_name.text.lower())
        elif f'{name} Bowling Records' in datas and f'{name} Fielding Records' in datas:
            overall_stats = {'Name':name,'Matches':[table[0]],'Runs':0,'50s':0,'100s':0,'bat_average':0,'bat_s/r':0,'bowl_matches':[table[0]],'bowl_overs':[table[1]],'Wickets':[table[2]],'bowl_average':[table[3]],'bowl_s/r':[table[4]],'bowl_economy':[table[5]], 'team':teams[0]}
            data = pd.DataFrame(overall_stats)
            data.replace(to_replace=r'-', value=0, inplace=True)
            data.replace(to_replace='', value=0, inplace=True)
            insert_data(data, match_type_name.text.lower())
        else:
            overall_stats = {'Name':[name],'Matches':[table[0]],'Runs':[table[1]],'50s':[table[2]],'100s':[table[3]],'bat_average':[table[4]],'bat_s/r':[table[5]],'bowl_matches':[table[6]],'bowl_overs':[table[7]],'Wickets':[table[8]],'bowl_average':[table[9]],'bowl_s/r':[table[10]],'bowl_economy':[table[11]], 'team':teams[0]}
            data = pd.DataFrame(overall_stats)
            data.replace(to_replace=r'-', value=0, inplace=True)
            data.replace(to_replace='', value=0, inplace=True)
            insert_data(data, match_type_name.text.lower())
    elif len(datas)==1:
        if f'{name} Batting Records' in datas:
            overall_stats = {'Name':[name],'Matches':[table[0]],'Runs':[table[1]],'50s':[table[2]],'100s':[table[3]],'bat_average':[table[4]],'bat_s/r':[table[5]],'bowl_matches':[table[0]],'bowl_overs':0,'Wickets':0,'bowl_average':0,'bowl_s/r':0,'bowl_economy':0, 'team':teams[0]}
            data = pd.DataFrame(overall_stats)
            data.replace(to_replace=r'-', value=0, inplace=True)
            data.replace(to_replace='', value=0, inplace=True)
            insert_data(data, match_type_name.text.lower())
        elif f'{name} Bowling Records' in datas:
            overall_stats = {'Name':name,'Matches':[table[0]],'Runs':0,'50s':0,'100s':0,'bat_average':0,'bat_s/r':0,'bowl_matches':[table[0]],'bowl_overs':[table[1]],'Wickets':[table[2]],'bowl_average':[table[3]],'bowl_s/r':[table[4]],'bowl_economy':[table[5]], 'team':teams[0]}
            data = pd.DataFrame(overall_stats)
            data.replace(to_replace=r'-', value=0, inplace=True)
            data.replace(to_replace='', value=0, inplace=True)
            insert_data(data, match_type_name.text.lower())
        else:
            overall_stats = {'Name':name,'Matches':[table[0]],'Runs':0,'50s':0,'100s':0,'bat_average':0,'bat_s/r':0, 'bowl_matches':[table[0]],'bowl_overs':0,'Wickets':0,'bowl_average':0,'bowl_s/r':0,'bowl_economy':0, 'team':teams[0]}
            data = pd.DataFrame(overall_stats)
            data.replace(to_replace=r'-', value=0, inplace=True)
            data.replace(to_replace='', value=0, inplace=True)
            insert_data(data, match_type_name.text.lower())
    else:
        overall_stats = {'Name':name,'Matches':[table[0]],'Runs':0,'50s':0,'100s':0,'bat_average':0,'bat_s/r':0, 'bowl_matches':[table[0]],'bowl_overs':0,'Wickets':0,'bowl_average':0,'bowl_s/r':0,'bowl_economy':0, 'team':teams[0]}
        data = pd.DataFrame(overall_stats)
        data.replace(to_replace=r'-', value=0, inplace=True)
        data.replace(to_replace='', value=0, inplace=True)
        insert_data(data, match_type_name.text.lower())
        print('No records found')


#this is going to send the set of data to records function
def data_collector2(driver,url,name):
        field = []
        tables = []
        url.click()
        for i in driver.find_elements(By.CLASS_NAME, 'tabButton'):
            for button in i.find_elements(By.TAG_NAME, 'button'):
                match_types.append(button)
        if len(match_types)!=0:
            for match in match_types:
                match.click()
                for element in driver.find_elements(By.CLASS_NAME, 'show'):
                    for k in element.find_elements(By.TAG_NAME, 'table'):
                        for table in k.find_elements(By.TAG_NAME, 'td'):
                            tables.append(table.text)          
                    for yk in element.find_elements(By.CLASS_NAME, 'hdgTxt3'):
                        field.append(yk.text)
                records(field, name, tables, match)
                tables.clear()
                field.clear()
            match_types.clear()
            driver.back()
            print(f'{name} stats scarapped and inserted successfully')
        else:
            print(f'No stats found for {name}')
            driver.back()


#this function is used to navigate thru webpage
def navigator(driver,page, team_number, team_name, matches):
    driver.get(f'https://www.hindustantimes.com/cricket/players/page-{page}?team={team_number}')
    button = driver.find_element(By.LINK_TEXT, 'Next')
    while button:
        data_collector1(page, team_number, team_name)
        if len(name)==0:
            print('No data found')
            break
        elements = driver.find_elements(By.CLASS_NAME, 'name')
        print(f'scrapping page-{page}')
        for j in range(len(elements)):
            url = driver.find_element(By.XPATH, f'//*[@id="__next"]/div[1]/div[4]/section/section/div[3]/div[3]/ul/li[{j+1}]/div[2]/span[1]/a')
            elements = data_collector2(driver,url,name[j])
        page+=1
        button = driver.find_element(By.LINK_TEXT, 'Next')
        data_psql.update(matches)
        data_psql.export(team_name)
        try:
            button.click()
        except:
            print(f'Team {team_name} scrapped successfully!!!')
            clear()
            break
        clear()


#this function is going to clear the data present in variables
def clear():
    name.clear()
    age.clear()
    role.clear()
    teams.clear()
    table.clear()