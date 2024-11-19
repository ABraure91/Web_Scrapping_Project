#Web Scrapping project

'''
    PURPOSE : This code scrap data from a proposed webpage. You can set wich field are about to be extracted. Data are stored in a mysql database.
    AUTHOR : A. BRAURE and A.MAO
    Last update DATE : 19/11/2024
    Copyright : Licence (c) BIOSAFE

    Input :
    - 

    Ouput : 
    - 

'''


#Libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlalchemy as sl


#Functions
def scrap_webpage_to_df(url):

    '''
        PURPOSE : 

        Input: 
        - 
    '''

    response = requests.get(url)

    soup = BeautifulSoup(response.text, 'html.parser')

    divs = soup.find_all("div")
    target_div = divs[1]
    rows = target_div.text.strip().split("\n")  # Diviser par ligne
    data = []

    for row in rows:
        fields = row.split()
        if len(fields) >= 3:
            data.append({
                "AC": fields[0],
                "ID": fields[1],
                "entry_type": fields[2],
            })

    database = pd.DataFrame(data)

    return database

def scrap_description(description_url_template, ac):

    '''
        PURPOSE : return the description associated to a specific AC.

        Input: 
        - 
    '''
    
    url = description_url_template + ac

    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    description_tag = soup.find("td", {"property": "schema:description"})

    return description_tag.get_text(strip=True) if description_tag else "No description available"

def description_dataframe(main_df, description_url_template):

    '''
        PURPOSE : Find the description associated to each AC and store couples AD/Description in a data frame.

        Input: 
        - 
    '''

    description_dataframe = pd.DataFrame(columns=["AC", "Description"])

    AC_list = list(main_df["AC"])

    for ac in AC_list:
        print(ac)
        collected_description = scrap_description(description_url_template, ac)
        description_dataframe.loc[len(description_dataframe)] = [ac, collected_description]

    return description_dataframe

def df_to_sql(df1, df2):

    '''
        PURPOSE : 

        Input: 
        - 
    '''

    # SQL Initialazation
    engine = sl.create_engine('sqlite:///:memory:')

    metadata = sl.MetaData()

    General_Informations = sl.Table('df1', metadata,
        sl.Column('AC', sl.String, primary_key=True),
        sl.Column('ID', sl.Integer),
        sl.Column('entry_type', sl.String)
    )

    Descriptions = sl.Table('df2', metadata,
        sl.Column('AC', sl.String, primary_key=True),
        sl.Column('description', sl.String),
        sl.ForeignKeyConstraint(['AC'], ['df1.AC'])
    )

    metadata.create_all(engine)

    # SQL fullfiling
    with engine.connect() as connection:
        df1.to_sql('df1', con=connection, if_exists='append', index=False)
        df2.to_sql('df2', con=connection, if_exists='append', index=False)


#Inputs
url = "https://prosite.expasy.org/cgi-bin/prosite/prosite_browse.cgi?order=hits%20desc&type=all"
description_url_template = "https://prosite.expasy.org/"


#Main
main_df = scrap_webpage_to_df(url)
main_df = main_df.iloc[5:].reset_index(drop=True)
main_df = main_df.iloc[len(main_df)-10:].reset_index(drop=True)
print(main_df)

description_df = description_dataframe(main_df, description_url_template)

df_to_sql(main_df, description_df)