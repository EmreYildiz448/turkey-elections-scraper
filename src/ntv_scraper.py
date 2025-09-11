#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import os
import bs4
import logging
import random
import time
import re
import io
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from threading import Lock
from unidecode import unidecode
from src.driver_utils import create_driver

def get_all_urls():
    sitemap_url = 'https://secim.ntv.com.tr/sitemap.xml'
    response = requests.get(sitemap_url)
    sitemap_soup = bs4.BeautifulSoup(response.content, 'xml')
    urls = [loc.text for loc in sitemap_soup.find_all('loc')]
    il_urls = urls[1:82]
    tum_ilceler_urls = urls[82:1055]
    merkez_ilce_urls = [url for url in tum_ilceler_urls if re.search(r'-merkez-', url)]
    ilce_urls = list(filter(lambda url: url not in merkez_ilce_urls, tum_ilceler_urls))
    belde_urls = [url for url in urls if re.search(r'-belde-', url)]
    tum_urls = il_urls + tum_ilceler_urls + belde_urls
    logging.info(f'Total province URLs: {len(il_urls)}')
    logging.info(f'Total county URLs: {len(tum_ilceler_urls)}')
    logging.info(f'Total central county URLs: {len(merkez_ilce_urls)}')
    logging.info(f'Total non-central county URLs: {len(ilce_urls)}')
    logging.info(f'Total town URLs: {len(belde_urls)}')
    logging.info(f'Total URLs: {len(tum_urls)}')
    return il_urls, tum_ilceler_urls, merkez_ilce_urls, ilce_urls, belde_urls, tum_urls

def scrape_to_df(url_list, party_list):
    
    def process_dataframe(df, party_list):
        if not df.empty:
            if 'OY ORANI' in df.columns:
                df['OY ORANI'] = df['OY ORANI'].str.replace('%', '').astype(float)
            if '2019 OY ORANI' in df.columns:
                df['2019 OY ORANI'] = df['2019 OY ORANI'].str.replace('%', '').astype(float)
            df.set_index('PARTİ', inplace=True)
            if 'SIRA' in df.columns:
                df.drop('SIRA', axis=1, inplace=True)
            df.rename(index=lambda x: unidecode(x).lower(), inplace=True)
            df.loc['bagimsiz toplam oy'] = 0
            for index in list(df.index):
                if index not in party_list:
                    df.loc['bagimsiz toplam oy', 'ALINAN OY'] += df.loc[index, 'ALINAN OY']
                    df.loc['bagimsiz toplam oy', 'OY ORANI'] += df.loc[index, 'OY ORANI']
                    if '2019 ALINAN OY' in df.columns and '2019 OY ORANI' in df.columns:
                        df.loc['bagimsiz toplam oy', '2019 ALINAN OY'] += df.loc[index, '2019 ALINAN OY']
                        df.loc['bagimsiz toplam oy', '2019 OY ORANI'] += df.loc[index, '2019 OY ORANI']
                    df.drop(index, inplace=True)
            df.rename(columns={'ALINAN OY': '2024 ALINAN OY', 'OY ORANI': '2024 OY ORANI'}, inplace=True)
        return df
    
    def scrape_single_url(url, driver_queue, lock, party_list):
        driver = driver_queue.get()
        try:
            driver.get(url)
            time.sleep(random.uniform(2, 4))
            wait = WebDriverWait(driver, 10)
            app_root = wait.until(EC.presence_of_element_located((By.TAG_NAME, "app-root")))
            content = app_root.get_attribute("innerHTML")
            dfs = pd.read_html(io.StringIO(content), thousands='.', decimal=',')
            prefix = url.split('/')[-1].replace('-secim-sonuclari', '').replace('-', '_')
            if "akarcay_gorumlu" in prefix:
                key_prefix = "tokat_almus_akarcay gorumlu_belde"
            elif '19' in prefix:
                key_prefix = "samsun_19 mayis"
            elif "-belde-" in url:
                try:
                    wait.until(EC.presence_of_all_elements_located((By.TAG_NAME, 'a')))
                    soup = bs4.BeautifulSoup(content, "lxml")
                    elements = soup.find_all('a')
                    if len(elements) > 9:
                        il_name = unidecode(elements[9].getText()).lower()
                        key_prefix = il_name + "_" + prefix
                    else:
                        raise ValueError(f'Not enough elements found for {url}')
                except Exception as e:
                    logging.error(f'Error processing elements for {url}: {e}')
                    return url, None, None
            else:
                key_prefix = prefix
            df_baskan = process_dataframe(dfs[2], party_list)
            df_meclis = process_dataframe(dfs[3], party_list)
            with lock:
                logging.info(f'{key_prefix} DataFrame created!')
            return key_prefix, df_baskan, df_meclis
        except Exception as e:
            with lock:
                logging.error(f'Error scraping {url}: {e}')
            return url, None, None
        finally:
            driver_queue.put(driver)

    data_dict, error_urls, empty_urls = {}, [], []
    driver_queue = Queue(maxsize=5)
    for _ in range(5):
        driver_queue.put(create_driver(deny_process=True))
    lock = Lock()

    executor = ThreadPoolExecutor(max_workers=5)  # Initialize executor
    try:
        future_to_url = {executor.submit(scrape_single_url, url, driver_queue, lock, party_list): url for url in url_list}
        for future in as_completed(future_to_url):
            try:
                key_prefix, df_baskan, df_meclis = future.result()
                if df_baskan is not None and not df_baskan.empty:
                    data_dict[f'{key_prefix}_baskanlik_sonuclari'] = df_baskan
                    data_dict[f'{key_prefix}_meclis_sonuclari'] = df_meclis
                    with lock:
                        logging.info(f'DataFrames for {future_to_url[future]} added!')
                else:
                    empty_urls.append(future_to_url[future])
            except Exception as exc:
                error_urls.append(future_to_url[future])
                with lock:
                    logging.error(f'URL generated an exception: {future_to_url[future]}: {exc}')
    finally:
        executor.shutdown(wait=True)  # Ensure all threads are terminated
        while not driver_queue.empty():
            driver = driver_queue.get()
            driver.close()
            driver.quit()
    return data_dict, error_urls, empty_urls

def remove_known_empty_urls(empty_urls, known_empty_urls):
    for url in known_empty_urls:
        if url in empty_urls:
            empty_urls.remove(url)

def retry_scraping(data_dict, error_urls, empty_urls, party_list, max_attempts=3):
    all_urls = error_urls + empty_urls
    attempt = 0
    while all_urls and attempt < max_attempts:
        new_data_dict, new_error_urls, new_empty_urls = scrape_to_df(all_urls, party_list)
        data_dict.update(new_data_dict)
        all_urls = new_error_urls + new_empty_urls
        attempt += 1
    if all_urls:
        logging.warning(f"Some URLs could not be scraped after {max_attempts} attempts: {all_urls}")
    return data_dict

def replace_empty_dataframes(votes, parties, candidates = None):
    values = votes
    values_sum = sum(values)
    if candidates:
        df = pd.DataFrame(columns=['PARTI', 'ADAY', '2024 ALINAN OY', '2024 OY ORANI'])
    else:
        df = pd.DataFrame(columns=['PARTI', '2024 ALINAN OY', '2024 OY ORANI'])
    df['PARTI'] = parties
    df['2024 ALINAN OY'] = values
    if candidates:
        df['ADAY'] = candidates
    df['2024 OY ORANI'] = df['2024 ALINAN OY'].apply(lambda x: round(x / values_sum * 100, 2))
    df.set_index('PARTI', inplace=True)
    return df

def separate_dictionary(data_dict):
    df_il, df_ilce, df_belde = {}, {}, {}
    il_count, ilce_count, belde_count = 0, 0, 0
    for key, df in data_dict.items():
        keysplit = key.split('_')
        if "belde" in keysplit:
            df_belde[key] = df
            belde_count += 1
        elif len(keysplit) == 4:
            df_ilce[key] = df
            ilce_count += 1
        elif len(keysplit) == 3:
            df_il[key] = df
            il_count += 1
    logging.info(f'Province count: {il_count/2} ({il_count} DataFrames in total, presidency & council results)')
    logging.info(f'County count: {ilce_count/2} ({ilce_count} DataFrames in total, presidency & council results)')
    logging.info(f'Town count: {belde_count/2} ({belde_count} DataFrames in total, presidency & council results)')
    df_aggregate_dict = [df_il, df_ilce, df_belde]
    return df_il, df_ilce, df_belde, df_aggregate_dict

