#!/usr/bin/env python3
"""
Queries Finhub API for US Equity book or trade data and saves pickled DataFrames of this data
in a symbol/day directory structure created by this script.
Uses Multiprocessing with an upper limit of 80% of cpu cores.  Finnhub API rate limit is divided
between these processes. 

Arguments:
--sym-file csv with 3 rows where
    first row: a comma seperated sequence of symbols
    second row: A single date in YYYY:MM:DD format to be used as the start date for all symbols
        Alternatively, a comma seperated sequence of start dates in YYYY:MM:DD format
        where each date corresponds to a symbol from the first row.
    third row: A single date in YYYY:MM:DD format to be used as the end date for all symbols (not inclusive)
        Alternatively, a comma seperated sequence of end dates (not inclusive) in YYYY:MM:DD format
        where each date corresponds to a symbol from the first row.
--type: choose between book data or trade data using 'nbbo' or 'tick'
--overwrite: bool to allow or disallow overwriting of existing files. default = False
--save_path: path to directory for saving symbol subdirectories and output files default = cwd
--api_key: Finnhub API key. Can be hardcoded in my_key parameter below imports in code

Returns:
Creates a directory in cwd with subdirs for each symbol
Saves a pickled Pandas DataFrame for each unique date-symbol combination

Uses Finnhub python API:
https://pypi.org/project/finnhub-python/
"""
__author__ = "Peter Cullen"
__email__ = "pcullen11@protonmail.com"
__copyright__ = "MIT"

import finnhub
import pandas as pd
import datetime
import argparse
import os
import time
import math
import multiprocessing as mp

# Finnhub Parameters
my_key = ""  # API key to be used by default
api_limit = 150  # 150 calls per min limit with basic plan
# Multiprocessing parameters
cores_to_use = math.floor(mp.cpu_count() * .8)  # Use ~80% of cpu cores
process_api_limit = math.floor(api_limit / cores_to_use)

# construct the argument parser
ap = argparse.ArgumentParser(description='Download data from Finnhub')
ap.add_argument("--sym-file", required=True, help="csv of symbols and date ranges")
ap.add_argument("--type", choices=['nbbo', 'tick'], required=True, help='nbbo or tick')
ap.add_argument("--overwrite", type=bool, default=False, help="overwrite existing data?")
ap.add_argument("--save-path", default=os.getcwd(), help="Path to save output files in")
ap.add_argument("--api-key",  default=my_key, help='Finnhub API key')

args = vars(ap.parse_args())
print(args)

# Create finnhub client object
finnhub_client = finnhub.Client(api_key=args["api_key"])

def load_sym_file(symbol_file_to_load):
    """
    Convert the sym_file csv into a dataframe and check for unique or global dates
    :param symbol_file_to_load: csv of symbols and dates
    :return: DataFrame with symbol as column names and dates as first two rows, flag to use one date for all
    """
    symbols = pd.read_csv(symbol_file_to_load)
    # If start and end dates only entered for the first symbol, use those for all
    date_na_flag = False
    for symbol in symbols:
        if pd.isna(symbols[symbol].iloc[0]):
            date_na_flag = True
            print(f"Using start date of {symbols.iloc[0,0]} for all symbols")
            break
    print(symbols)
    return symbols, date_na_flag

def check_save_path_format(path_to_save):
    """
    Add / at end of entered save_path if missing
    :param path_to_save: save-path argument passed by user
    :return: reformatted save_path if needed
    """
    if path_to_save != os.getcwd():
        if path_to_save[-1] != '/':
            path_to_save = path_to_save + '/'
    return path_to_save


def build_file_list(sym_df, one_date_flag):
    """
    Convert df of symbols, start dates, and end dates to a list of unique symbol, date pairs
    :param sym_df: DataFrame of symbols, start dates, and end dates
    :param one_date_flag: If True, start and end dates are used globally
    :return: List of tuples, one for each symbol, date pair (symbol, date)
    """
    fetch_list = []  # List of (symbol, date) tuples to be returned
    for symbol in sym_df.columns:
        if not one_date_flag:
            start_date = datetime.datetime.strptime(sym_df[symbol].iloc[0], "%Y-%m-%d")
            # If end date is not entered, only query start date
            try:
                end_date = datetime.datetime.strptime(sym_df[symbol].iloc[1], "%Y-%m-%d")
            except:
                end_date = start_date
                print(f"No end date entered for {symbol}. Querying one day.")
        else:
            start_date = datetime.datetime.strptime(sym_df.iloc[0, 0], "%Y-%m-%d")
            # If end date is not entered, only query start date
            try:
                end_date = datetime.datetime.strptime(sym_df.iloc[1, 0], "%Y-%m-%d")
            except:
                end_date = start_date
                print("No end date entered. Querying start date only.")
        # Create a list of all dates in range
        date_list = [(start_date + datetime.timedelta(days=x)).strftime("%Y-%m-%d")
                     for x in range(0, max((end_date - start_date).days, 1))]
        # Add each date along with the symbol to the file list
        for date in date_list:
            fetch_list.append((symbol, date))
    return fetch_list

def make_directory(sym, make_dir):
    """
    Optionally create directory for symbol if it doesn't exist
    Modify path to save to include symbol subdirectory
    :param sym: Symbol being queried
    :param make_dir: If True, create the directory. Otherwise, just modify the path
    :return: Modified path to reflect symbol subdirectory
    """
    cwd_path = os.getcwd()
    if args['type'] == 'nbbo':
        # if default path is used, create a db_raw directory
        if args['save_path'] == cwd_path:
            newpath = args['save_path'] + '/db_raw/' + sym + '/'
        else:
            newpath = args['save_path'] + sym + '/'
    elif args['type'] == 'tick':
        if args['save_path'] == cwd_path:
            newpath = args['save_path'] + '/db_raw_tick/' + sym + '/'
        else:
            newpath = args['save_path'] + sym + '/'
    if (not os.path.exists(newpath)) & make_dir:
        os.makedirs(newpath)
        print(f'First time adding data for {sym}')
    return newpath

def name_file(sd_tuple):
    """
    Names file with symbol_date.pkl convention
    Checks if file already exists and if overwrite is allowed
    :param sd_tuple: (symbol, date) pair being queried
    :return: Final save path and filename, flag to stop if file exists and no overwrite allowed
    """
    # df columns of same symbol have a sym.1 sym.2 format when reading csv
    # this allows multiple date ranges for one symbol but won't allow querying of symbols with . in name
    sym = sd_tuple[0].split(".")[0]
    day = sd_tuple[1]
    stop_write = False  # Flag to querying data for this pair
    # Add symbol subdirectory to path
    newpath = make_directory(sym, make_dir=False)
    # check for existing file for the date
    newpath = newpath + sym + "_" + day + '.pkl'
    if not os.path.exists(newpath):
        print(f"Processing {sd_tuple} data...")
    elif args['overwrite']:
        print(f'Overwriting {sd_tuple} data...')
    else:
        stop_write = True  # used to prevent overwriting an existing file
    return newpath, stop_write

def append_time_list(tl, st):
    '''
    Updates elapsed times in time list
    Adds most recent time
    Resets start time to current time
    :param tl: time_list of recent api query times
    :param st: start time of last attempt
    :returns tl: updated time list
    :returns st: updated start time
    '''
    time_delta = int(time.perf_counter() - st)
    tl = list(map(lambda x: x + time_delta, tl))
    tl.append(math.ceil(time.perf_counter() - st))
    st = time.perf_counter()
    return tl, st


def check_time_list(tl, sd):
    """
    Removes entries in time list more than one minute old.
    Checks query count in last minute and sleeps if threshold hit
    :param tl: time_list of recent api query times
    :param sd: symbol date pair being queried
    """
    while (tl[0] >= 60) & (len(tl) > 0):
        tl.pop(0)
    if len(tl) > process_api_limit:
        time.sleep(60 - tl[0])
        print(f'{sd} Reached process API limit, sleeping {60 - tl[0]} seconds.')


def query_finnhub(sd_pair):
    """
    Attempt to gather all data for the symbol date pair parameter passed in.
    Save the pickled DataFrame if successful
    :param sd_pair: Tuple of (symbol, date) to query
    :return: Saves pickled dataframe in save-path
    """
    symbol = sd_pair[0]
    date = sd_pair[1]
    empty = False  # Flag if empty frame is returned
    # exception_flag = False #6_1 delete
    newpath, stop_write = name_file(sd_pair)
    if stop_write:
        print(f'Skipping {sd_pair} to avoid overwrite. use --overwrite True to override')
        return
    count = 0  # number of rows collected from finnhub for this symbol-date pair
    total = 1  # total is a finnhub name denoting total rows for that symbol-date pair
    nskip = 25000  # finnhub returns a max of 25000 rows per query
    df = pd.DataFrame()
    runs = 0
    # populate api rate control list to help correct for it resetting since last sd_pair finished
    time_list = [int(60/process_api_limit) * x for x in range(process_api_limit, 1, -1)]

    start_time = time.perf_counter()
    while count < total:
        try:
            if args['type'] == 'nbbo':
                temp_df = pd.DataFrame(finnhub_client.stock_nbbo(symbol,
                                                                 date, 25000, skip=nskip * runs))
            elif args['type'] == 'tick':
                temp_df = pd.DataFrame(finnhub_client.stock_tick(symbol,
                                                                 date, 25000, skip=nskip * runs))
            if (temp_df.shape[0] == 0):
                empty = True  # empty dfs will not be saved
                print(f"{sd_pair} Empty frame returned from API call. Skipping this symbol_date pair")
                # When most files already exist, we query many non-trading days which return empty frame
                # ... very quickly and then hit API limit. This sleep addresses that.
                time.sleep(3)
                break

            df = pd.concat((df, temp_df))
            runs += 1
            # future change: make count based on row in df to protect against change in rows returned per query
            count = 25000 * runs
            total = temp_df.iloc[-1]['total']
            print(f'{sd_pair} Successful query number {runs}')
            time_list, start_time = append_time_list(time_list, start_time)
            check_time_list(time_list, sd_pair)

        except Exception as e:
            time_list, start_time = append_time_list(time_list, start_time)
            check_time_list(time_list, sd_pair)
            if '429' in str(e):
                print(f'{sd_pair}************ Finnhub API Limit Exception. Sleeping 15 seconds ************')
                time.sleep(15)
            elif 'read timeout' in str(e):
                print(sd_pair, " ", e)
                time.sleep(1)
            else:
                print(f'{sd_pair} returned Exception: ', str(e)[:100], "\n",
                      'Sleeping for 5 seconds to avoid spamming')
                time.sleep(5)

    if not empty:
        # protocol=4 helps prevent compatibility issues when loading pickle later
        df.to_pickle(newpath, protocol=4)
        print(f'{sd_pair} ++++++++++++++++ Saved with {df.shape[0]} rows  ++++++++++++++++')
        if df.shape[0] != total:
            print(f'{sd_pair} *********** WARNING: DF ROW COUNT != FINNHUB TOTAL ***********')


def main():
    args['save_path'] = check_save_path_format(args['save_path'])
    symbols, one_date_for_all = load_sym_file(args['sym_file'])
    for s in symbols.columns:
        make_directory(s, make_dir=True)
    sym_date_pairs = build_file_list(symbols, one_date_for_all)

    # Single process version
    # process_api_limit = api_limit  # allow single process to use full api limit
    # for sdp in sym_date_pairs:
    #     query_finnhub(sdp)

    # Version with Multiprocessing
    print(f'Multiprocessing using {cores_to_use} cores')
    pool = mp.Pool(cores_to_use)
    pool.map(query_finnhub, sym_date_pairs)


if __name__ == "__main__":
    main()
