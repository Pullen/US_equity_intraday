# US_equity_intraday
This first stage of this project takes maximally granular historic book or trade data from the Finnhub API and organizes this data into individual days grouped by symbol.

### query_finnhub_usequity.py  
Queries Finhub API for US Equity book or trade data and saves pickled DataFrames of this data
in a symbol/day directory structure created by this script.  

Uses Multiprocessing with an upper limit of 80% of cpu cores.  Finnhub API rate limit is divided
between these processes.  

__Arguments:__  

-sym-file csv with 3 rows where
    first row: a comma seperated sequence of symbols  
    
    second row: A single date in YYYY:MM:DD format to be used as the start date for all symbols
        Alternatively, a comma seperated sequence of start dates in YYYY:MM:DD format
        where each date corresponds to a symbol from the first row.  
        
    third row: A single date in YYYY:MM:DD format to be used as the end date for all symbols (not inclusive)
        Alternatively, a comma seperated sequence of end dates (not inclusive) in YYYY:MM:DD format
        where each date corresponds to a symbol from the first row.  
        
-type: choose between book data or trade data using 'nbbo' or 'tick'  

--overwrite: bool to allow or disallow overwriting of existing files. default = False
--save_path: path to directory for saving symbol subdirectories and output files default = cwd
--api_key: Finnhub API key. Can be hardcoded in my_key parameter below imports in code
Returns:
Creates a directory in cwd with subdirs for each symbol
Saves a pickled Pandas DataFrame for each unique date-symbol combination
Uses Finnhub python API:
https://pypi.org/project/finnhub-python/
