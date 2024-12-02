import requests
from bs4 import BeautifulSoup
from pandas import DataFrame, read_csv
from numpy.random import randint
from numpy import sort
from dask import dataframe as dd

import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from os import listdir
from os.path import isfile, join
from tqdm import tqdm

def fetch_and_save_reactor_details(index):
    try:
        web = f'https://pris.iaea.org/PRIS/CountryStatistics/ReactorDetails.aspx?current={index}'
        page = requests.get(web)
        soup = BeautifulSoup(page.content, 'html.parser')
        if soup.find(id='MainContent_MainContent_lblReactorStatus').text != 'Under Construction':
            df_rx = pd.DataFrame(columns=['Country', 'Reactor', 'Type', 'Status', 'Year', 'Electricity Supplied [GW.h]', 'Annual Time On Line [h]', 'Source'])
            for i, tr in enumerate(soup.find_all('tbody')[0].find_all('tr')):
                try:
                    name = soup.find(id='MainContent_MainContent_lblReactorName').text
                    df_rx.loc[i] = [soup.find(id='MainContent_litCaption').text.strip(),
                                    soup.find(id='MainContent_MainContent_lblReactorName').text,
                                    soup.find(id='MainContent_MainContent_lblType').text.strip(),
                                    soup.find(id='MainContent_MainContent_lblReactorStatus').text,
                                    int(tr.find_all('td')[0].text.strip()),
                                    float(tr.find_all('td')[1].text.strip()),
                                    float(tr.find_all('td')[3].text.strip()),
                                    web]
                except Exception as e:
                    # print(f"Error processing table row: {e}")
                    pass
            df_rx.to_csv(f'data/rx_{name}.csv')
    except Exception as e:
        # print(f"Error fetching reactor details for index {index}: {e}")
        pass

def parallel_fetch_and_save():
    indices = range(1100) # 1110 is the number of reactors in the PRIS database, as of 2024-12-01
    with ThreadPoolExecutor(max_workers=48) as executor:
        list(tqdm(executor.map(fetch_and_save_reactor_details, indices), total=len(indices)))

# Execute the parallel fetching and saving
if __name__ == '__main__':

    parallel_fetch_and_save()

    df = dd.read_csv('data/rx_*.csv')
    df = df.compute()
    df.drop(columns='Unnamed: 0', inplace=True)
    df.reset_index(drop=True, inplace=True)
    df.sort_values(by=['Country', 'Reactor', 'Year'], inplace=True)
    # df = df.loc[(df['Year'] >= 2017) & (df['Year'] <= 2020)]
    df = df.loc[df['Annual Time On Line [h]'] > 0]
    df.reset_index(drop=True, inplace=True)
    df.to_csv('data/reactors_all.csv')

# Execute the parallel fetching and saving
