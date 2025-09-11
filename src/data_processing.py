#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
import logging
import gc
import re
import operator
import pandas as pd
import numpy as np
from collections import defaultdict
from pathlib import Path
from unidecode import unidecode

def set_index_if_exists(df, index_column, file_path):
    if index_column in df.columns:
        df.set_index(index_column, inplace=True)
    else:
        logging.debug(f"'{index_column}' column not found for {file_path}.")
    return df

def drop_columns_if_exists(df, columns_to_drop):
    for column in columns_to_drop:
        if column in df.columns:
            df.drop(column, axis=1, inplace=True)

def excel_to_df(path_to_folder='excel_files'):
    # Adjust the path to always point to the correct location
    script_loc = Path(__file__).resolve().parent.parent
    folder_path = script_loc / path_to_folder

    def get_excel_names(cwd=folder_path):
        # Use the resolved folder path
        return [file.name for file in Path(cwd).glob('*.xlsx')]

    # Fetch Excel file names from the resolved folder
    file_names_list = get_excel_names()
    df_dict = {}

    for file_name in file_names_list:
        file_path = folder_path / file_name
        try:
            excel_file = pd.ExcelFile(file_path)
            sheet_list = excel_file.sheet_names
            for sheet in sheet_list:
                df = pd.read_excel(excel_file, sheet_name=sheet)
                
                # Standardize column names
                df.columns = [unidecode(col.strip()).upper() for col in df.columns]
                
                # Set 'PARTI' column as index if available
                df = set_index_if_exists(df, 'PARTI', file_path)

                df_dict[sheet] = df
        
        except Exception as e:
            logging.error(f"Error reading {file_name}: {e}")
    
    gc.collect()
    print("All DataFrames successfully imported from Excel files.")
    return df_dict

def dataframe_ysk_update(dataframes_corrected, dataframes_corrective, party_list, bb_list, year):
    diff_dict = {}
    if isinstance(party_list, dict):
        party_loop = [(party19, party24) for party19, party24 in party_list.items()]
    elif isinstance(party_list, set):
        party_loop = [(party, party) for party in party_list]
    filter_dict_dfparser = {
        '2019': {
            'party_loop': party_loop,
            'vote_columns': ['2019 ALINAN OY', '2019 OY ORANI'],
            'town_check': lambda df24, checkwords: df24.assign(**{'2019 ALINAN OY': 0, '2019 OY ORANI': 0}) if 'belde' in checkwords else df24
        },
        '2024': {
            'party_loop': party_loop,
            'vote_columns': ['2024 ALINAN OY', '2024 OY ORANI'],
            'town_check': lambda df24, checkwords: df24
        }
    }

    selected_town_check = filter_dict_dfparser[year]['town_check']
    selected_party_loop = filter_dict_dfparser[year]['party_loop']
    selected_votes_received = filter_dict_dfparser[year]['vote_columns'][0]
    selected_votes_percentage = filter_dict_dfparser[year]['vote_columns'][1]

    def get_general_value(dataframes_corrective, key, checkwords, party):
        try:
            if party in dataframes_corrective[key].columns:
                return dataframes_corrective[key].at[checkwords[0], party]
        except Exception as e:
            logging.error(e)
        return 0

    def get_county_value(dataframes_corrective, checkwords, party):
        try:
            ilce_check = f'{checkwords[0]} {checkwords[1]}' if checkwords[1] == 'merkez' else checkwords[1]
            key_suffix = f'_belediye_baskanligi' if checkwords[2] == 'baskanlik' else f'_belediye_meclisi'
            key = f'{checkwords[0]}{key_suffix}'
            if party in dataframes_corrective[key].columns:
                return dataframes_corrective[key].at[ilce_check, party]
        except Exception as e:
            logging.error(e)
        return 0

    def get_town_value(dataframes_corrective, checkwords, party):
        belde_key = f'{checkwords[0]} {checkwords[1]} - {checkwords[2]}' if checkwords[1] == 'merkez' else f'{checkwords[1]} - {checkwords[2]}'
        try:
            if 'baskanlik' in (checkwords[4], checkwords[5]):
                key = f'{checkwords[0]}_belediye_baskanligi'
            elif 'meclis' in (checkwords[4], checkwords[5]):
                key = f'{checkwords[0]}_belediye_meclisi'
            else:
                return 0
            
            if party in dataframes_corrective[key].columns:
                return dataframes_corrective[key].at[belde_key, party]
        except KeyError:
            logging.error(f'{belde_key} not found in data.')
        return 0
    
    def get_num(dataframes_corrective, checkwords, party, bb_list):
        num = 0
        
        if len(checkwords) == 3:
            # Handle general cases based on checkwords length and bb_list
            if checkwords[0] in bb_list:
                if checkwords[1] == 'baskanlik':
                    num = get_general_value(dataframes_corrective, 'buyuksehir_baskanligi_genel', checkwords, party)
                elif checkwords[1] == 'meclis':
                    num = get_general_value(dataframes_corrective, 'belediye_meclisi_genel', checkwords, party)
            elif checkwords[0] not in bb_list and checkwords[1] == 'meclis':
                num = get_general_value(dataframes_corrective, 'il_meclisi_genel', checkwords, party)
    
        elif 'belde' not in checkwords:
            # Handle municipality-specific cases where 'belde' (town) is not involved (i.e county cases)
            num = get_county_value(dataframes_corrective, checkwords, party)
    
        elif "belde" in checkwords:
            # Handle 'belde' specific cases with try-except block
            num = get_town_value(dataframes_corrective, checkwords, party)
        
        return num

    def add_missing_parties(df, party_loop):
        # Find missing parties not already in the index
        missing_parties = [party24 for _, party24 in party_loop if party24 not in df.index]
        
        if missing_parties:
            # Create a DataFrame for missing parties with zeros
            missing_df = pd.DataFrame(0, index=missing_parties, columns=df.columns)
            
            # Preserve the index name
            missing_df.index.name = df.index.name
            
            # Concatenate the original DataFrame with the new one
            df = pd.concat([df, missing_df])
        
        return df
    
    def update_vote_counts(df, dataframes_corrective, checkwords, party_loop, bb_list, oy_column, diff_dict, key):
        for party19, party24 in party_loop:
            num = get_num(dataframes_corrective, checkwords, party19, bb_list)
            old_num = df.at[party24, oy_column]
            difference = old_num - num
            df.at[party24, oy_column] = num
            diff_dict[f'{key} - {party24}'] = difference
    
            if difference != 0:
                logging.debug(f"{key}: Value for {party24} changed from {old_num} to {num} (total difference is {difference})")
    
    # Refactored loop
    for key, df24 in dataframes_corrected.items():
        checkwords = key.split('_')
        df24 = (selected_town_check)(df24, checkwords)
        
        # Add missing parties and reassign to df24
        df24 = add_missing_parties(df24, selected_party_loop)
    
        # Update vote counts and track differences
        update_vote_counts(df24, dataframes_corrective, checkwords, selected_party_loop, bb_list,
                           selected_votes_received, diff_dict, key)
        
        # Transform np.nan to 0 and drop rows with only 0
        df24.replace(np.nan, 0, inplace=True)
        df24 = df24.loc[df24.any(axis=1)]
        # Fill percentage column based on numeric column
        df24[selected_votes_percentage] = (
            df24[selected_votes_received]
            .div(df24[selected_votes_received].sum())
            .mul(100)
            .round(2)
        )
        dataframes_corrected[key] = df24
    return diff_dict

def df_subpart_update(updater, updated):
    for key1, df1 in updater.items():
        for key2, df2 in updated.items():
            if key1 == key2:
                updated[key2] = df1
    return updated

# Main Function to Write DataFrames to Excel Files
def df_to_excel(df_dict, df_dict_ilce, df_dict_belde, script_loc):

    # Helper Function to Parse Key Parts
    def parse_key_parts(key):
        parts = key.split('_')
        return parts[0], parts[-2]  # province prefix, type of data (e.g., "baskanlik" or similar)
    
    # Helper Function to Group DataFrames by Prefix and Type
    def group_by_prefix_and_type(df_dict):
        grouped = defaultdict(list)
        for key, df in df_dict.items():
            prefix, key_type = parse_key_parts(key)
            grouped[(prefix, key_type)].append((key, df))
        return grouped
    
    # Helper Function to Add Sheets to Excel Writer
    def add_sheets_to_writer(writer, df_dict, il_key_prefix, il_key_type, added_sheets):
        # Add sheets to the Excel writer object based on prefix and type match
        for key, df in df_dict.get((il_key_prefix, il_key_type), []):
            # Handle "merkez" case where county-level data replaces province-level sheet
            if "_merkez_" in key and "_baskanlik_" in key and len(key.split('_')) == 4:
                sheet_name = f'{il_key_prefix}_{il_key_type}_sonuclari'
                df.to_excel(writer, sheet_name=sheet_name)
                added_sheets.add(il_key_prefix)
                logging.debug(f"{key} added as {sheet_name} to {il_key_prefix} (central county case)")
            else:
                # Handle sheet name conflicts by adding "_duplicate" if necessary
                sheet_name = key if key not in added_sheets else f"{key}_duplicate"
                df.to_excel(writer, sheet_name=sheet_name)
                added_sheets.add(sheet_name)
                logging.debug(f"{key} added as a sheet to {il_key_prefix}!")
        
    folder_name = 'excel_files'
    folder_path = script_loc / folder_name
    folder_path.mkdir(parents=True, exist_ok=True)
    logging.info(f'Folder "{folder_name}" created at {folder_path}.')

    # Group ilce and belde DataFrames by prefix and type
    grouped_ilce = group_by_prefix_and_type(df_dict_ilce)
    grouped_belde = group_by_prefix_and_type(df_dict_belde)

    for il_key, il_df in df_dict.items():
        # Extract prefix and type for the current province key
        il_key_prefix, il_key_type = parse_key_parts(il_key)

        try:
            # Create Excel writer for each main province (il)
            file_path = folder_path / f"{il_key}.xlsx"
            with pd.ExcelWriter(file_path, mode="w") as writer:
                # Write the main province DataFrame to the Excel file
                il_df.to_excel(writer, sheet_name=il_key)
                logging.info(f"{il_key} Excel file created!")
                
                # Track sheets that have been added to avoid naming conflicts
                added_sheets = set()

                # Add ilce (district) sheets to the Excel file
                add_sheets_to_writer(writer, grouped_ilce, il_key_prefix, il_key_type, added_sheets)

                # Add belde (town) sheets to the Excel file
                add_sheets_to_writer(writer, grouped_belde, il_key_prefix, il_key_type, added_sheets)

        except Exception as e:
            # Log an error message if the file writing fails for any reason
            logging.error(f"Failed to create Excel file for {il_key}: {e}")

    # Log completion and collect garbage to free up memory
    logging.info('All DataFrames exported to Excel files successfully!')
    gc.collect()

def excel_to_df_ysk(folder_path):
    subfolder_type = ["belediye_baskanligi", "belediye_meclisi", "il_meclisi", "buyuksehir_baskanligi"]
    subfolder_count = ["1", "2", "3", "4", "5"]
    df_dict = {}
    df_dict_statistics = {}
    unique_party_set = set(['ak parti', 'chp', 'iyi parti', 'saadet'])

    def party_translator(unique_party_set):
        party_translator = {}
        for item in unique_party_set:
            if item == 'hdp':
                party_translator[item] = 'dem parti'
            elif item == 'bbp':
                party_translator[item] = 'buyuk birlik'
            else:
                party_translator[item] = item
        return party_translator
    
    def process_ysk_dataframe(df, unique_party_set, subfolder, file_path):
        df.dropna(axis=1, how='all', inplace=True)
        df.dropna(axis=0, how='all', inplace=True)
        df.rename(columns=lambda x: unidecode(x).lower(), inplace=True)
        df = set_index_if_exists(df, 'ilce adi', file_path)
        df = set_index_if_exists(df, 'il adi', file_path)
        df.drop('Oy Oranı', inplace=True)
        
        # Drop unnecessary columns using helper function
        drop_columns_if_exists(df, ['ilce id', 'il id'])
        
        df.rename(index=lambda x: unidecode(x).lower(), inplace=True)
        
        for column in list(df.columns):
            df[column] = df[column].astype(float)
        df_new = df[['kayitli secmen sayisi', 'oy kullanan secmen sayisi', 'gecerli oy toplami']].copy()
        drop_columns_if_exists(df, ['kayitli secmen sayisi', 'oy kullanan secmen sayisi', 'gecerli oy toplami'])
        
        if 'bagimsiz toplam oy' not in list(df.columns):
            df['bagimsiz toplam oy'] = 0
            for column in list(df.columns):
                if column not in unique_party_set:
                    df['bagimsiz toplam oy'] += df[column]
                    df.drop(column, axis=1, inplace=True)
        else:
            unique_party_set.update(list(df.columns))
        return df, df_new
        
    for subfolder in subfolder_type:
        for count in subfolder_count:
            try:
                directory_path = folder_path / subfolder / count
                for file_path in directory_path.iterdir():
                    if file_path.is_file():
                        dataframe_name = '_'.join(file_path.stem.split('_')[1:-1])
                    try:
                        dfs = pd.read_html(file_path, thousands='.', decimal=',')
                        df_processed, stat_df = process_ysk_dataframe(dfs[0], unique_party_set, subfolder, file_path)
                        df_dict[dataframe_name] = df_processed
                        df_dict_statistics[dataframe_name] = stat_df
                        logging.info(f"Successfully read file: {file_path}")
                    except Exception as e:
                        logging.error(f"Failed to read file: {file_path} with error: {e}")
            except FileNotFoundError as e:
                logging.error(f'File not found: {e}')
                continue
    empty_check = 0
    for name, df in df_dict.items():
        if df.empty:
            logging.error(f'Empty DF found! Index: {name}')
            empty_check += 1
    if empty_check == 0:
        logging.info("All's well.")
    else:
        logging.warning('Someone is empty!')
    partytranslation = party_translator(unique_party_set)
    return df_dict, df_dict_statistics, unique_party_set, partytranslation

def remove_empty_province_dfs(dataframes_full, dataframes_il, il_list):
    for il in il_list:
        for df_dict in [dataframes_full, dataframes_il]:
            df_dict.pop(f'{il}_baskanlik_sonuclari', None)
            if f'{il}_baskanlik_sonuclari' not in df_dict:
                logging.warning(f'{il} is already absent, moving on...')
            else:
                logging.info(f"{il} dropped")

def find_shortcoming_2019(df_dict):
    expected_shortcomings = ['afyonkarahisar_sinanpasa_guney_belde_baskanlik_sonuclari', 
                             'afyonkarahisar_sinanpasa_guney_belde_meclis_sonuclari', 
                             'cankiri_orta_dodurga_belde_baskanlik_sonuclari', 
                             'cankiri_orta_kalfat_belde_baskanlik_sonuclari', 
                             'cankiri_orta_dodurga_belde_meclis_sonuclari', 
                             'cankiri_orta_kalfat_belde_meclis_sonuclari', 
                             'tokat_resadiye_demircili_belde_baskanlik_sonuclari', 
                             'tokat_resadiye_demircili_belde_meclis_sonuclari']
    missing_2019_results = {}
    for key, df in df_dict.items():
        if '2019 OY ORANI' in df.columns:
            value = df['2019 OY ORANI'].sum()
            missing = round(100 - value, 2)
            if value < 100 and missing > 1:
                missing_2019_results[key] = missing
        else:
            logging.error(f"Missing column '2019 OY ORANI' in {key}")
    
    sorted_dict = dict(sorted(missing_2019_results.items(), key=lambda item: item[1]))
    
    if sorted(list(sorted_dict.keys())) == sorted(expected_shortcomings):
        logging.info("8 expected missing DataFrames detected. No data for the listed municipalities in 2019:")
        logging.info(expected_shortcomings)
    else:
        logging.warning('Unexpected missing DataFrames detected:')
        logging.warning(list(sorted_dict.keys()))

def councilor_dict_update(data_dict):
    party_abbreviation_dict = {'adalet ve kalkinma partisi': 'ak parti',
                               'anavatan partisi': 'anap',
                               'bagimsiz': 'bagimsiz toplam oy',
                               'bagimsiz turkiye partisi': 'btp',
                               'buyuk birlik partisi': 'buyuk birlik',
                               'cumhuriyet halk partisi': 'chp',
                               'demokrasi ve atilim partisi': 'deva partisi',
                               'demokrat parti': 'dp',
                               'demokratik sol parti': 'dsp',
                               'emek partisi': 'emep',
                               'gelecek partisi': 'gelecek partisi',
                               'halklarin demokratik partisi': 'dem parti',
                               'halklarin esitlik ve demokrasi partisi': 'dem parti',
                               'hur dava partisi': 'huda par',
                               'iyi parti': 'iyi parti',
                               'memleket partisi': 'memleket',
                               'milli yol partisi': 'milli yol',
                               'milliyetci hareket partisi': 'mhp',
                               'saadet partisi': 'saadet',
                               'sol parti': 'sol parti',
                               'turkiye isci partisi': 'tip',
                               'turkiye komunist partisi': 'tkp',
                               'vatan partisi': 'vatan partisi',
                               'yeniden refah partisi': 'yeniden refah'}
    # Helper function for getting party abbreviation
    def get_party_abbreviation(party, abbreviation_dict):
        return abbreviation_dict.get(party, party)

    # Update data_dict with abbreviations and convert votes
    updated_data_dict = {}
    for year, party_dict in data_dict.items():
        updated_party_dict = {}
        for party, votes in party_dict.items():
            try:
                updated_party_dict[get_party_abbreviation(party, party_abbreviation_dict)] = int(votes.replace('.', '').replace(',', ''))
            except ValueError:
                logging.error(f"Failed to convert votes for party {party}: {votes}")
        updated_data_dict[year] = updated_party_dict

    # Create summary dictionaries for council member counts
    belediye_meclis_uye_sayilari = {
        f'{year} MECLİS ÜYESİ SAYISI': updated_data_dict[f'{year}_belediye_meclisi']
        for year in ['2019', '2024']
    }

    il_meclis_uye_sayilari = {
        f'{year} MECLİS ÜYESİ SAYISI': updated_data_dict[f'{year}_il_meclisi']
        for year in ['2019', '2024']
    }

    return updated_data_dict, belediye_meclis_uye_sayilari, il_meclis_uye_sayilari

def results_per_municipality_df(municipality_data, election_type, year, script_loc, save_file=False, alliances=False):

    full_municipality_list = municipality_data.full_list
    bb_list = municipality_data.bb_list
    il_list = municipality_data.il_list
    df_dict = municipality_data.dataframes_full_pull
    dataframes_2019 = municipality_data.dataframes_2019
    dataframes_2024 = municipality_data.dataframes_2024
    stats_19 = municipality_data.stats_19
    stats_24 = municipality_data.stats_24
    full_province_list = municipality_data.full_province_list
    sege_ilce = municipality_data.sege_ilce
    sege_il = municipality_data.sege_il
    party_list = municipality_data.party_list

    def baskanlik_action(summary_df, list):
        for item in list:
            summary_df.loc[item, 'merkez', '-'] = summary_df.loc[item, '-', '-']
            summary_df.loc[item, '(ilceler toplami)', '-'] = baskanlik_selected_df['belediye_baskanligi_genel'].loc[item]
            summary_df.drop((item, '-', '-'), inplace=True)

    def meclis_action(summary_df, list):
        for item in list:
            summary_df.loc[item, '(ilceler toplami)', '-'] = summary_df.loc[item, '-', '-']
            summary_df.drop((item, '-', '-'), inplace=True)
    
    baskanlik_df_year_filter = {'2019': dataframes_2019,
                               '2024': dataframes_2024}
    stats_df_year_filter = {'2019': stats_19, 
                           '2024': stats_24}
    main_filter_dict = {'baskanlik': ['_belediye_baskanligi', bb_list, 'buyuksehir_baskanligi_genel', baskanlik_action, il_list, 'belediye_baskanligi_genel'],
                   'meclis': ['_belediye_meclisi', il_list, 'il_meclisi_genel', meclis_action, bb_list, 'belediye_meclisi_genel']}
    
    baskanlik_selected_df = baskanlik_df_year_filter[year]
    stats_selected_df = stats_df_year_filter[year]
    selected_election_type_lowerlevel = main_filter_dict[election_type][0]
    selected_municipality_list = main_filter_dict[election_type][1]
    selected_election_type_upperlevel = main_filter_dict[election_type][2]
    selected_action = main_filter_dict[election_type][3]
    selected_aggregation_list = main_filter_dict[election_type][4]
    selected_election_type_general = main_filter_dict[election_type][5]

    def extract_location_parts(item, indexers, defaults, conditions):
        """
        Extracts province, county, and town from an item based on dynamic conditions.
    
        Parameters:
        - item: A standardized list of strings.
        - indexers: Tuple specifying index positions for province, county, and town.
        - defaults: Tuple specifying default values for province, county, and town.
        - conditions: Tuple of conditions for province, county, and town where:
            - Each condition is a tuple (operator, value) to compare len(item) against.
    
        Returns:
        - province, county, town
        """
    
        # Unpack indexers, defaults, and conditions
        province_indexer, county_indexer, town_indexer = indexers
        province_default, county_default, town_default = defaults
        province_condition, county_condition, town_condition = conditions

        # Calculate length of the item
        item_len = len(item)
    
        # Apply conditions and extract values or defaults
        province = item[province_indexer] if province_condition[0](item_len, province_condition[1]) else province_default
        county = item[county_indexer] if county_condition[0](item_len, county_condition[1]) else county_default
        town = item[town_indexer] if town_condition[0](item_len, town_condition[1]) else town_default
    
        return province, county, town
    
    def create_framework_df(full_municipality_list, party_list):
        df_province_list = []
        df_county_list = []
        df_town_list = []
        votes = 0
        for item in full_municipality_list:
            if isinstance(item, str):
                item = [item]
            elif isinstance(item, tuple):
                item = list(item)
            current_province, current_county, town = extract_location_parts(
                item=item,
                indexers=(0, 1, 2),
                defaults=["-", "-", "-"],
                conditions=((operator.gt, 0),(operator.gt, 1),(operator.gt, 2))
            )
            df_province_list.append(current_province)
            df_county_list.append(current_county)
            df_town_list.append(town)
        df = pd.DataFrame({'Province': df_province_list, 'County': df_county_list, 'Town': df_town_list})
        for party in party_list:
            df[party] = 0
        df = df.sort_values(by=['Province', 'County', 'Town'], key=lambda col: col.map(lambda x: '' if x == '-' else x))
        df.set_index(['Province', 'County', 'Town'], inplace=True)
        return df

    def extract_county_town(item):
        new_item = item.replace('-', '').replace('  ', ' ')
        new_item_list = new_item.split(' ')
        if new_item_list == ['19', 'mayis']:
            new_item_list = ['19 mayis']
        elif new_item_list == ['almus', 'akarcay', 'gorumlu']:
            new_item_list = ['almus', 'akarcay gorumlu']
        item_len = len(new_item_list)
        if item_len == 2 and "merkez" in new_item_list:
            county = "merkez"
            town = '-'
        else:
            _, county, town = extract_location_parts(
                item=new_item_list,
                indexers=(0, 0, -1),
                defaults=[None, "merkez", "-"],
                conditions=((operator.eq, -1),(operator.le, 2),(operator.gt, 1))
            )
        return county, town

    def get_voter_stats(df, item):
        kayitli_secmen = df.at[item, 'kayitli secmen sayisi']
        toplam_verilen_oy = df.at[item, 'oy kullanan secmen sayisi']
        toplam_gecerli_oy = df.at[item, 'gecerli oy toplami']
        return kayitli_secmen, toplam_verilen_oy, toplam_gecerli_oy
    
    def create_summary_checklist():
        summary_checklist_dict = {}
    
        for prvnc in full_province_list:
            df = stats_selected_df[f'{prvnc}{selected_election_type_lowerlevel}']
            for item in df.index:
                county, town = extract_county_town(item)
                kayitli_secmen, toplam_verilen_oy, toplam_gecerli_oy = get_voter_stats(df, item)
                summary_checklist_dict[(prvnc, county, town)] = [kayitli_secmen, toplam_verilen_oy, toplam_gecerli_oy]
    
        for item in selected_municipality_list:
            kayitli_secmen, toplam_verilen_oy, toplam_gecerli_oy = get_voter_stats(
                stats_selected_df[selected_election_type_upperlevel], item)
            summary_checklist_dict[(item, '-', '-')] = [kayitli_secmen, toplam_verilen_oy, toplam_gecerli_oy]
    
        for item in selected_aggregation_list:
            kayitli_secmen, toplam_verilen_oy, toplam_gecerli_oy = get_voter_stats(
                stats_selected_df[selected_election_type_general], item)
            summary_checklist_dict[(item, '(ilceler toplami)', '-')] = [kayitli_secmen, toplam_verilen_oy, toplam_gecerli_oy]
    
        return summary_checklist_dict

    def insert_sege_scores(sege_df, skor_column, kademe_column):
        """
        Inserts SEGE scores into summary_df.
    
        Parameters:
        - sege_df: DataFrame containing SEGE data (either il or ilçe level).
        - skor_column: Column name to insert SEGE score values.
        - kademe_column: Column name to insert SEGE kademe values.
        """
        result = {key: list(val.values()) for key, val in sege_df.to_dict(orient='index').items()}
        for key, value in result.items():
            summary_df.loc[key, skor_column] = value[0]
            summary_df.loc[key, kademe_column] = value[1]

    def df_separate_counties():
        summary_df_copy = summary_df.copy(deep=True)
        mask = summary_df.index.isin([(item, '-', '-') for item in selected_municipality_list])
        dropped_rows_df = summary_df_copy[mask]
        remaining_rows_df = summary_df_copy[~mask]
        return remaining_rows_df, dropped_rows_df

    def calculate_majority_vote(row):
        for col in parti_basina_hesap_sutunlari:
            if row[col] / row['gecerli oy toplami'] * 100 > 50:
                return col
        return None

    def include_alliances():
        # Define party lists for each alliance
        alliance_parties = {
            'cumhur ittifakı': ['ak parti', 'mhp', 'buyuk birlik', 'dsp', 'huda par'],
            'millet ittifakı': ['chp', 'iyi parti', 'gelecek partisi', 'dp', 'deva partisi', 'saadet']
        }
        
        # Condition mask for rows where 'salt cogunluk' is NaN
        condition_mask = summary_df['salt cogunluk'].isna()
    
        # Iterate through each alliance to apply the relevant mask and update 'salt cogunluk'
        for alliance_name, parties in alliance_parties.items():
            alliance_mask = summary_df.filter(items=parties).sum(axis=1) >= summary_df['gecerli oy toplami'] / 2
            final_mask = alliance_mask & condition_mask
            summary_df.loc[final_mask, 'salt cogunluk'] = alliance_name

    def include_region_info(script_loc):
        region_file_loc = script_loc / "SehirlerBolgeler.xlsx"
        with pd.ExcelFile(region_file_loc) as excel_file:
            ex_df = pd.read_excel(excel_file)
        region_to_city_dict = ex_df.groupby("BolgeAd").sum().to_dict()["SehirAd"]
        for key, value in region_to_city_dict.items():
            decodednames = unidecode(value)
            region_to_city_dict[key] = re.split('(?<=.)(?=[A-Z])', decodednames)
        city_to_region_dict = {city.lower(): region.lower().replace(' bölgesi', '') for region, cities in region_to_city_dict.items() for city in cities}
        return city_to_region_dict

    def save_summary_to_excel(summary_df, summary_df_ilceler, summary_df_iller, election_type, year, script_loc):
        """
        Saves the summary DataFrames to Excel files.
    
        Parameters:
        - summary_df: The main summary DataFrame.
        - summary_df_ilceler: DataFrame containing ilce-level information.
        - summary_df_iller: DataFrame containing il-level information.
        - election_type: Type of election ('baskanlik' or 'meclis').
        - year: Year of the election (e.g., '2019', '2024').
        - script_loc: Base location for saving files.
        """
        folder_name = 'municipal_summary'
        folder_path = script_loc / folder_name
        folder_path.mkdir(parents=True, exist_ok=True)
        file_path = folder_path / f'{election_type}_summary_df_{year}.xlsx'
        with pd.ExcelWriter(file_path) as writer:
            summary_df.to_excel(writer, sheet_name=f"{election_type}_summary_df_{year}")
            summary_df_ilceler.to_excel(writer, sheet_name=f"{election_type}_ilceler_summary_df_{year}")
            summary_df_iller.to_excel(writer, sheet_name=f"{election_type}_iller_summary_df_{year}")
        logging.info(f'{election_type}_summary_df_{year}, {election_type}_ilceler_summary_df_{year}, and {election_type}_iller_summary_df_{year} created!')
    
    summary_df = create_framework_df(full_municipality_list, party_list)

    # Collect all the updates in a list of dictionaries
    update_records = []
    
    for key, df in df_dict.items():
        # Extracting province, county, and town from key
        parts = key.split('_')
        if election_type in parts:
            province, county, town = extract_location_parts(
                item=parts,
                indexers=(0, 1, 2),
                defaults=["-", "-", "-"],
                conditions=((operator.gt, 0),(operator.ge, 4),(operator.eq, 6))
            )
            for party in df.index:
                oy_value = df.at[party, f'{year} ALINAN OY']
                if party in summary_df.columns:
                    update_records.append({
                        'Province': province,
                        'County': county,
                        'Town': town,
                        'Party': party,
                        'Value': oy_value
                    })
    
    # Convert collected updates into a DataFrame
    updates_df = pd.DataFrame(update_records)
    
    # Use `.pivot_table()` to create a structured DataFrame for applying updates in bulk
    pivot_df = updates_df.pivot_table(index=['Province', 'County', 'Town'], columns='Party', values='Value', aggfunc='sum').fillna(0)
    
    # Apply updates to `summary_df` using vectorized addition
    summary_df = summary_df.add(pivot_df, fill_value=0)

    # Apply selected action to create and fill "ilceler toplami" column
    selected_action(summary_df, selected_aggregation_list)
    
    # Drop rows that are completely zero and fill missing values
    summary_df = summary_df.loc[(summary_df != 0).any(axis=1)].fillna(0)
    
    # Initialize specific columns with 0.0 or np.nan
    kwargs = {
        "kayitli secmen sayisi": 0.0,
        "oy kullanan secmen sayisi": 0.0,
        "gecerli oy toplami": 0.0,
        "SEGE il skor": np.nan,
        "SEGE il kademe": np.nan,
        "SEGE ilce skor": np.nan,
        "SEGE ilce kademe": np.nan
    }
    summary_df = summary_df.assign(**kwargs)

    # Create and assign population statistics columns
    summary_checklist_dict = create_summary_checklist()
    summary_checklist_df = pd.DataFrame.from_dict(summary_checklist_dict, 
                                                  orient='index', 
                                                  columns=['kayitli secmen sayisi', 'oy kullanan secmen sayisi', 'gecerli oy toplami'])
    summary_df.update(summary_checklist_df)
    summary_df.sort_index(inplace=True)

    # Insert SEGE scores to appropriate columns
    insert_sege_scores(sege_ilce, 'SEGE ilce skor', 'SEGE ilce kademe')
    insert_sege_scores(sege_il, 'SEGE il skor', 'SEGE il kademe')

    # Create and fill "kazanan parti" column
    parti_basina_hesap_sutunlari = summary_df.columns[0:-7]
    summary_df['kazanan parti'] = summary_df[parti_basina_hesap_sutunlari].idxmax(axis=1)
    
    # Hardcoded logic to fix known data errors
    if election_type == 'baskanlik' and year == '2024':
        summary_df.at[('izmir', 'selcuk', '-'), 'kazanan parti'] = 'chp'
    elif election_type == 'baskanlik' and year == '2019':
        summary_df.at[('bitlis', 'ahlat', 'ovakisla'), 'kazanan parti'] = 'saadet'
        
    # Create and fill "salt cogunluk" column for council summary
    if election_type == 'meclis':
        salt_cogunluk_mask = summary_df[parti_basina_hesap_sutunlari].div(summary_df['gecerli oy toplami'], axis=0).mul(100)
        summary_df['salt cogunluk'] = salt_cogunluk_mask.apply(lambda row: row.idxmax() if row.max() > 50 else None, axis=1)
        
        # Modify "salt cogunluk" to include alliance majority
        if alliances:
            include_alliances()
            
    # Create and fill "katilim_orani" and "hata_orani" columns
    kwargs = {
        "katilim orani": (summary_df['oy kullanan secmen sayisi']
                       .div(summary_df['kayitli secmen sayisi'])
                       .mul(100)
                       .round(2)),
        "hata orani": ((summary_df['oy kullanan secmen sayisi']
                     .sub(summary_df['gecerli oy toplami']))
                    .div(summary_df['oy kullanan secmen sayisi'])
                    .mul(100)
                    .round(2))
    }
    summary_df = summary_df.assign(**kwargs)
    
    # Create and fill region column
    city_to_region_dict = include_region_info(script_loc)
    summary_df['bolge'] = summary_df.index.get_level_values(0).map(city_to_region_dict.get)

    # Drop all columns that have all values as zero or np.nan
    summary_df = summary_df.loc[:, (summary_df != 0).any(axis=0)]

    # Separate the main DataFrame into smaller DataFrames based on municipality level
    summary_df_ilceler, summary_df_iller = df_separate_counties()

    # Save the created DataFrames as Excel files
    if save_file:
        save_summary_to_excel(summary_df, summary_df_ilceler, summary_df_iller, election_type, year, script_loc)
    return summary_df, summary_df_ilceler, summary_df_iller

def summary_election_results(election_data, party_list, summary_type, script_loc, save_file=True, metropolis_list=None):

    b_ilce_sum_2024 = election_data.b_ilce_sum_2024
    b_ilce_sum_2019 = election_data.b_ilce_sum_2019
    b_buy_sum_2024 = election_data.b_buy_sum_2024
    b_buy_sum_2019 = election_data.b_buy_sum_2019
    m_ilce_sum_2019 = election_data.m_ilce_sum_2019
    m_ilce_sum_2024 = election_data.m_ilce_sum_2024
    m_il_sum_2019 = election_data.m_il_sum_2019
    m_il_sum_2024 = election_data.m_il_sum_2024
    bel_m_uye = election_data.belediye_meclis_uye_sayilari
    il_m_uye = election_data.il_meclis_uye_sayilari
    df_dict = election_data.dataframes_full_pull

    sum_df = pd.DataFrame(index=party_list, columns=['2024 OY', '2019 OY', '2024 OY ORANI', '2019 OY ORANI'], dtype='float64').fillna(0)
    name_list = []
    
    filter_conditions = {
        'genel_ozet': lambda name, name_split: (name_split[0] in metropolis_list and name_split[1] == "baskanlik") 
                    or (name_split[0] not in metropolis_list and name_split[1] ==  "meclis"),
        'buyuksehir_baskanligi': lambda name, name_split: len(name_split) == 3 and "meclis" not in name and name_split[0] in metropolis_list,
        'belediye_baskanligi': lambda name, name_split: 'baskanlik' in name and (name_split[0] not in metropolis_list or name_split[0] in metropolis_list and len(name_split) > 3),
        'belediye_meclisleri': lambda name, name_split: 'meclis' in name and (name_split[0] not in metropolis_list and len(name_split) >= 4 or name_split[0] in metropolis_list and len(name_split) == 3),
        'il_meclisleri': lambda name, name_split: 'meclis' in name and len(name_split) == 3 and name_split[0] not in metropolis_list
    }
    
    if summary_type not in filter_conditions:
        raise ValueError(f"Unknown summary type: {summary_type}")

    selected_summary_type = filter_conditions[summary_type]
        
    # Collecting all relevant data to update sum_df
    update_records = []

    for name, df in df_dict.items():
        name_split = name.split('_')
        if selected_summary_type(name, name_split):
            name_list.append(name)
            for party in df.index:
                if party in party_list:
                    update_records.append({'Party': party, '2024 OY': df['2024 ALINAN OY'][party], '2019 OY': df['2019 ALINAN OY'][party]})
                elif party not in party_list:  # Handling "bagimsiz" (independent) cases
                    update_records.append({'Party': 'bagimsiz', '2024 OY': df['2024 ALINAN OY'][party], '2019 OY': df['2019 ALINAN OY'][party]})


    # Convert collected updates into a DataFrame for efficient aggregation
    updates_df = pd.DataFrame(update_records)

    # Use groupby and sum to aggregate votes per party
    aggregated_df = updates_df.groupby('Party').sum()

    # Update sum_df based on aggregated_df
    sum_df.update(aggregated_df)

    # Calculate and store total votes
    total24 = sum_df['2024 OY'].sum()
    total19 = sum_df['2019 OY'].sum()
    
    def round_percentages(series, total):
        unrounded = (series / total) * 100
        rounded = unrounded.round(2)
        error = 100 - rounded.sum()
        if error != 0:
            max_idx = rounded.idxmax()
            rounded[max_idx] += error
        return rounded
        
    sum_df['2024 OY ORANI'] = round_percentages(sum_df['2024 OY'], total24)
    sum_df['2019 OY ORANI'] = round_percentages(sum_df['2019 OY'], total19)

    def create_mapper_dicts():
        county_municipality_counts_2024 = b_ilce_sum_2024["kazanan parti"].value_counts().to_dict()
        county_municipality_counts_2019 = b_ilce_sum_2019["kazanan parti"].value_counts().to_dict()
        ilce_belediye_mapper_dict = {'2024 BELEDIYE SAYISI': county_municipality_counts_2024,
                                     '2019 BELEDIYE SAYISI': county_municipality_counts_2019}
        metropolis_municipality_counts_2024 = b_buy_sum_2024["kazanan parti"].value_counts().to_dict()
        metropolis_municipality_counts_2019 = b_buy_sum_2019["kazanan parti"].value_counts().to_dict()
        buyuksehir_belediye_mapper_dict = {'2024 BELEDIYE SAYISI': metropolis_municipality_counts_2024,
                                           '2019 BELEDIYE SAYISI': metropolis_municipality_counts_2019}
        return ilce_belediye_mapper_dict, buyuksehir_belediye_mapper_dict

    ilce_belediye_mapper_dict, buyuksehir_belediye_mapper_dict = create_mapper_dicts()
    
    mapper_filter_dict = {'buyuksehir_baskanligi': buyuksehir_belediye_mapper_dict,
                          'belediye_baskanligi': ilce_belediye_mapper_dict,
                          'belediye_meclisleri': bel_m_uye,
                          'il_meclisleri': il_m_uye}
    
    if summary_type != 'genel_ozet':
        for key, value in mapper_filter_dict[summary_type].items():
            sum_df[key] = sum_df.index.map(value).fillna(0).astype(int)

    def meclis_salt_cogunluk():
        belediye_meclis_2019_salt_hesabi = m_ilce_sum_2019[m_ilce_sum_2019['salt cogunluk'].notnull()]['salt cogunluk'].value_counts().to_dict()
        belediye_meclis_2024_salt_hesabi = m_ilce_sum_2024[m_ilce_sum_2024['salt cogunluk'].notnull()]['salt cogunluk'].value_counts().to_dict()
        belediye_meclis_absmaj_dict = {'2024 SALT COGUNLUK': belediye_meclis_2024_salt_hesabi,
                                            '2019 SALT COGUNLUK': belediye_meclis_2019_salt_hesabi}
        il_meclisleri_2019_salt_hesabi = m_il_sum_2019[m_il_sum_2019['salt cogunluk'].notnull()]['salt cogunluk'].value_counts().to_dict()
        il_meclisleri_2024_salt_hesabi = m_il_sum_2024[m_il_sum_2024['salt cogunluk'].notnull()]['salt cogunluk'].value_counts().to_dict()
        il_meclisleri_absmaj_mapper_dict = {'2024 SALT COGUNLUK': il_meclisleri_2024_salt_hesabi,
                                          '2019 SALT COGUNLUK': il_meclisleri_2019_salt_hesabi}
        absolute_majority_mapper_dict = {'belediye_meclisleri': belediye_meclis_absmaj_dict,
                                         'il_meclisleri': il_meclisleri_absmaj_mapper_dict}
        return absolute_majority_mapper_dict

    salt_cogunluk_mapper_dict = meclis_salt_cogunluk()
    
    if 'meclis' in summary_type:
        for key, value in salt_cogunluk_mapper_dict[summary_type].items():
            sum_df[key] = sum_df.index.map(value).fillna(0).astype(int)
    
    def save_to_excel(df, summary_type, script_loc):
        folder_name = 'excel_files'
        subfolder_name = 'general_results'
        folder_path = script_loc / folder_name / subfolder_name
        folder_path.mkdir(parents=True, exist_ok=True)
        file_name = f'{summary_type}_sonuclar.xlsx'
        file_path = folder_path / file_name
        with pd.ExcelWriter(file_path) as writer:
            df.to_excel(writer)
        logging.info(f'Excel file for {summary_type} results created at {folder_path}, named {file_path}')

    if save_file:
        save_to_excel(sum_df, summary_type, script_loc)
    logging.info(f'{summary_type} created!')
    return sum_df, name_list

