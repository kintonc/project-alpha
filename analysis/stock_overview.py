# ==============================
# Import libraries, logging
# ==============================

import yfinance as yf
import pandas as pd
import glob # get list of files in folder, and filter by file extension
import os

# Logging
import logging, logging.config
logging.config.fileConfig(fname='../logger.ini', disable_existing_loggers=False)
# Comment/uncomment to see INFO LEVEL (__name__ debugger) or DEBUG level statements
logger = logging.getLogger(__name__)
# logger = logging.getLogger('debug_level') # uncomment if you need to see DEBUG LEVEL statements

# ==============================
# Get list of CSV files from "data" folder
# ==============================
data_folder = '../../data/reference'
os.chdir(data_folder)
csv_files = []
for file in glob.glob("*.csv"):
    # Filter out all "detailed.csv" files
    if 'detailed' not in file:
        file = file.replace('.csv', '')
        csv_files.append(file)
logger.debug(csv_files)


# ==============================
# Pull detailed stock info from YFinance
# ==============================
# For every csv file:
for file in csv_files:
    stock_list = pd.read_csv(file + '.csv');
    stock_list.columns = ['stock']

    detailed_stock_info = pd.DataFrame(columns=['name', 'sector', 'industry', 'country', 'mktCap', 'fullTimeEmployees'])
    counter = 0

    # For every stock:
    for index, row in stock_list.iterrows():
        counter += 1
        ticker = row['stock']

        # Display progress
        logger.info(file + ': ' + str(counter) + '/' + str(len(stock_list)) + ': ' + ticker)

        # Pull data from YFinance, store in array
        ticker_yf = yf.Ticker(ticker)
        try:
            detailed_stock_info.loc[row['stock']] = [ticker_yf.info['shortName'], ticker_yf.info['sector'],
                                                     ticker_yf.info['industry'], ticker_yf.info['country'],
                                                     ticker_yf.info['marketCap'], ticker_yf.info['fullTimeEmployees']]
        # If there was an error from YFinance stock pull
        # (which usually means YFinance doesn't have data for a partcular column, ex: fullTimeEmployees)
        except Exception as e:
            error_str = str(e)
            logger.error(file + ': ' + str(counter) + '/' + str(len(stock_list)) + ': ' + ticker + ' - ERROR: ' + error_str)

            info_columns = ['shortName','sector','industry','country','marketCap','fullTimeEmployees']

            # regularMarketOpen error means ticker was delisted
            if error_str == "'regularMarketOpen'":
                detailed_stock_info.loc[ticker] = ['delisted', None, None, None, None, None]
            else:
                # check if the error was due to a column not having data
                column_error = False
                for column in info_columns:
                    if error_str in "'" + column + "'":
                        column_error = True
                        break

                # if one of the info_columns does not have data and threw an error (ex: error was "sector"),
                # try to add data in other columns into the df
                if column_error == True:
                    row = []

                    for column in info_columns:
                        # If this column (ex: "sector") was the error, append None to the row
                        if error_str == ("'" + column + "'"):
                            row.append(None)
                        else:
                            # Try adding the "info" column, if error, add "None"
                            try:
                                row.append(ticker_yf.info[column])
                            except Exception as e:
                                row.append(None)

                    # Add this stock's data to df
                    detailed_stock_info.loc[ticker] = row
                else:
                    detailed_stock_info.loc[ticker] = ['error', None, None, None, None, None]

    detailed_stock_info.to_csv(file + '_detailed_latest.csv')
    logger.debug(detailed_stock_info)