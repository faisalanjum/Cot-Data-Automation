# https://github.com/NDelventhal/cot_reports#data-release
# The data, which is generally released each Friday at 3:30 pm Eastern time, comes with a lag of three days, as the reported data is typically from previous Tuesday (close).
from pandas.tseries.offsets import BDay
from datetime import date
import pandas_market_calendars as mcal
import cot_reports as cot
import pandas as pd
import numpy as np
from common import save_to_dir
from backend.db.models import *
from backend.db.migrate_db import clean_db, migrate_models
import logging
from pathlib import Path
logging.basicConfig(filename="logs/cot.log",level=logging.DEBUG)
logger= logging.getLogger(__name__)
import numpy as np
from pathlib import Path
import cot_functions as cot # assuming this module is imported elsewhere
from typing import Optional

# Docstring for function
def get_legacy_fut_opt(market_code: str, report_type: Optional[str] = "opt", hist: Optional[bool] = True) -> None:
    """
    Download and get COT legacy futures/options data for a given market code, and optionally store it locally.

    Args:
        market_code: A string representing the CFTC contract market code for the desired data.
        report_type: A string indicating the type of data to download ("opt" or "fut").
            Defaults to "opt".
        hist: A boolean indicating whether to compare downloaded data to previously stored data and only keep new data.
            If False, all downloaded data is kept.
            Defaults to True.
    Returns:
        None. The function saves the downloaded data to a CSV file in a specified directory.

    """
    # Download and filter data based on report_type and market_code
    if report_type == "fut":
        legacy_fut = cot.cot_all(cot_report_type='legacy_fut')
        legacy_df = legacy_fut[legacy_fut['CFTC Contract Market Code'] == market_code]
        legacy_df["Report_Type"] = "legacy_fut"
        filename = "legacy_fut_" + market_code + ".csv"
    elif report_type == "opt":
        legacy_futopt = cot.cot_all(cot_report_type='legacy_futopt')
        legacy_df = legacy_futopt[legacy_futopt['CFTC Contract Market Code'] == market_code]
        legacy_df["Report_Type"] = "legacy_futopt"
        filename = "legacy_futopt_" + market_code + ".csv"

    # Clean up column names and format date/time information
    legacy_df.columns = legacy_df.columns.str.replace(' ', '_').str.replace("(", "").str.replace(")", "") \
        .str.replace("-", "_").str.replace("%", "Per").str.replace("=", "")
    legacy_df.sort_values(by='As_of_Date_in_Form_YYYY_MM_DD', ascending=True, inplace=True)
    legacy_df.set_index('As_of_Date_in_Form_YYYY_MM_DD', inplace=True)
    legacy_df = legacy_df.replace(".", np.nan)
    legacy_df.index.name = 'Date'
    legacy_df["Market_Code"] = market_code
    legacy_df.reset_index(inplace=True)

    # Check if historical data is to be kept or not, and store new data if applicable
    if not hist or hist == False:
        read_path = (Path(__file__).resolve().parent).joinpath("FILEDB", filename)
        compare_df = pd.read_csv(read_path)
        compare_df = compare_df.drop_duplicates()
        legacy_df.reset_index(inplace=True)
        legacy_df = legacy_df[~legacy_df["Date"].isin(compare_df["Date"])]

    # Save data to file
    save_to_dir(df=legacy_df, directory="FILEDB", filename=filename, mapper=Legacy)


def get_supplemental(market_code: str) -> pd.DataFrame:
    """
    Downloads and returns data from the CFTC's Supplemental Commitments of Traders (COT) report for a given market.

    Args:
        market_code (str): The CFTC contract market code for the desired market (e.g. "00639B").

    Returns:
        pd.DataFrame: A pandas DataFrame containing the Supplemental COT data for the specified market.
    """
    supplemental_futopt = cot.cot_all(cot_report_type='supplemental_futopt')
    supplemental_futopt = supplemental_futopt[supplemental_futopt['CFTC_Contract_Market_Code'] == market_code]
    return supplemental_futopt


def get_financial_futures(market_code: str) -> pd.DataFrame:
    """
    Downloads and returns data from the CFTC's Traders in Financial Futures (TFF) report for a given financial futures market.

    Args:
        market_code (str): The CFTC contract market code for the desired financial futures market (e.g. "0233T").

    Returns:
        pd.DataFrame: A pandas DataFrame containing the TFF data for the specified financial futures market.
    """
    # Note: there is no TFF data available for lumber futures
    traders_in_financial_futures_fut = cot.cot_all(cot_report_type='traders_in_financial_futures_fut')
    traders_in_financial_futures_fut = traders_in_financial_futures_fut[traders_in_financial_futures_fut['CFTC_Contract_Market_Code'] == market_code]
    return traders_in_financial_futures_fut





from cot import cot

def get_financial_futures_options(market_code: str) -> pd.DataFrame:
    """
    Downloads and returns data from the CFTC's Traders in Financial Futures (TFF) report for a given financial futures options market.

    Args:
        market_code (str): The CFTC contract market code for the desired financial futures options market (e.g. "GEF").

    Returns:
        pd.DataFrame: A pandas DataFrame containing the TFF data for the specified financial futures options market.
    """
    traders_in_financial_futures_futopt = cot.cot_all(cot_report_type='traders_in_financial_futures_futopt')
    traders_in_financial_futures_futopt = traders_in_financial_futures_futopt[traders_in_financial_futures_futopt['CFTC_Contract_Market_Code'] == market_code]
    return traders_in_financial_futures_futopt


def clean_disagg_fut_opt(market_code, columns_to_keep=None, fut_opt='opt'):
    """
    Cleans the disaggregated futures and options data for the specified market code.

    Args:
        market_code (str): The CFTC market code for the desired market.
        columns_to_keep (list, optional): List of columns to keep. Defaults to None, which keeps all columns.
        fut_opt (str, optional): Type of data to clean, either 'fut' for futures or 'opt' for options. Defaults to 'opt'.

    Returns:
        pandas.DataFrame: The cleaned disaggregated futures and options data.
    """
    
    # Select the appropriate data based on fut_opt value
    if fut_opt == "opt":
        disaggregated_futopt = cot.cot_all(cot_report_type='disaggregated_futopt')
    elif fut_opt == "fut":
        disaggregated_futopt = cot.cot_all(cot_report_type='disaggregated_fut')
    else:
        print("Wrong_choice for fut_opt")
        return None
    
    # Filter data by market code, sort and set index to date
    disaggregated_futopt = disaggregated_futopt[disaggregated_futopt['CFTC_Contract_Market_Code']==market_code]
    disaggregated_futopt.sort_values(by='Report_Date_as_YYYY-MM-DD', ascending=True, inplace=True)
    disaggregated_futopt.set_index('Report_Date_as_YYYY-MM-DD', inplace=True)
    disaggregated_futopt.index.name = 'Date'
    
    # Keep only the desired columns, if specified
    if columns_to_keep:
        disaggregated_futopt = disaggregated_futopt[columns_to_keep]
    
    # Calculate net speculative length and percent of open interest for commercial and non-commercial traders
    if fut_opt == "opt":
        disaggregated_futopt['Net_Spec_Length_Cal'] = disaggregated_futopt['M_Money_Positions_Long_All'] - disaggregated_futopt['M_Money_Positions_Short_All']
        disaggregated_futopt['Pct_of_OI_MM_NSL'] = disaggregated_futopt['Pct_of_OI_M_Money_Long_All'] - disaggregated_futopt['Pct_of_OI_M_Money_Short_All']
    elif fut_opt == "fut":
        disaggregated_futopt['Net_Spec_Length'] = disaggregated_futopt['M_Money_Positions_Long_All'] - disaggregated_futopt['M_Money_Positions_Short_All']
        disaggregated_futopt['Pct_of_OI_MM_NSL'] = disaggregated_futopt['Pct_of_OI_M_Money_Long_All'] - disaggregated_futopt['Pct_of_OI_M_Money_Short_All']
    
    # Convert index to datetime and sort in descending order
    disaggregated_futopt.index = pd.DatetimeIndex(disaggregated_futopt.index)
    disaggregated_futopt.sort_index(ascending=False, inplace=True)
    
    # Replace dots with NaNs and add market code column
    disaggregated_futopt = disaggregated_futopt.replace('.', np.nan)
    disaggregated_futopt["Market_Code"] = market_code
    
    return disaggregated_futopt


def delayed_cot_df(lag=4):
    _ , disaggregated_futopt = clean_disagg_fut_opt()
    cot_df = disaggregated_futopt.copy(deep=True)
    cot_df.index = cot_df.index.map(lambda x : x + lag*BDay())
    cot_df = cot_df.resample('B').ffill()
    cot_df.sort_index(ascending=False, inplace=True)
    last_row_values = cot_df.sort_index(ascending=False).iloc[0]

    yesterday = pd.to_datetime(date.today().strftime('%Y-%m-%d')) - pd.DateOffset(days=1)
    today = pd.to_datetime(date.today().strftime('%Y-%m-%d'))
    cme = mcal.get_calendar("CME_Agriculture")
    extra_days = cme.valid_days(start_date=pd.to_datetime(cot_df.sort_index(ascending=False).index[0]) + pd.DateOffset(days=1), end_date=yesterday) # can change it to today also if required

    extra_days = extra_days.strftime('%Y-%m-%d')

    for new_dt in extra_days:
        for col in cot_df.columns: cot_df.loc[new_dt,col] = last_row_values[col]

    cot_df.sort_index(ascending=False, inplace=True)

    return cot_df


def populate_data(market_code: str, columns_to_keep: list[str] = None, hist: bool = True, fut_opt: str = "opt",
                  file_to_save: str = "disaggregated-futures-options") -> None:
    """
    Populates the data for a specified market code and type of data (futures/options) by cleaning and preprocessing
    the data, saving it to a file, and comparing it to existing data if needed.

    Args:
        market_code (str): Market code to retrieve data for.
        columns_to_keep (List[str], optional): List of columns to keep in the resulting DataFrame. Defaults to None.
        hist (bool, optional): Whether to save the data to a historical file and compare to existing data. Defaults to True.
        fut_opt (str, optional): Type of data to retrieve, either "fut" for futures or "opt" for options. Defaults to "opt".
        file_to_save (str, optional): Name of the file to save the data to. Defaults to "disaggregated-futures-options".
    """
    df = clean_disagg_fut_opt(market_code=market_code, fut_opt=fut_opt, columns_to_keep=columns_to_keep)
    df.reset_index(inplace=True)
    if fut_opt == "opt":
        f_name = file_to_save + "_" + market_code + "_opt.csv"
        df["Report_Type"] = "disaggregated_futopt"
    elif fut_opt == "fut":
        f_name = file_to_save + "_" + market_code + "_fut.csv"
        df["Report_Type"] = "disaggregated_fut"
    if hist:
        save_to_dir(df=df, directory="FILEDB", filename=f_name, mapper=DisaggregatedFuturesOptions)
    else:
        read_path = (Path(__file__).resolve().parent).joinpath("FILEDB", f_name)
        compare_df = pd.read_csv(read_path)
        compare_df = compare_df.drop_duplicates()
        df = df[~df["Date"].isin(compare_df["Date"])]
        save_to_dir(df=df, directory="FILEDB", filename=f_name, mapper=DisaggregatedFuturesOptions)



logger.info("Running cod script on {}".format(date.today()))
try:
    logger.info("cleaning cot data")
    clean_db()
    logger.info("migrating cot tables")
    migrate_models()
    Lumber_code = '058643'
    columns_to_keep =['Open_Interest_All','Prod_Merc_Positions_Long_All','Prod_Merc_Positions_Short_All','M_Money_Positions_Long_All','M_Money_Positions_Short_All','M_Money_Positions_Spread_All','Tot_Rept_Positions_Long_All','Tot_Rept_Positions_Short_All','Change_in_Open_Interest_All','Change_in_Prod_Merc_Long_All','Change_in_Prod_Merc_Short_All','Change_in_M_Money_Long_All','Change_in_M_Money_Short_All','Pct_of_Open_Interest_All','Pct_of_OI_Prod_Merc_Long_All','Pct_of_OI_Prod_Merc_Short_All','Pct_of_OI_M_Money_Long_All','Pct_of_OI_M_Money_Short_All','Pct_of_OI_M_Money_Spread_All']
    populate_data(market_code=Lumber_code,columns_to_keep=None,hist=True,fut_opt="opt")
    populate_data(market_code=Lumber_code,columns_to_keep=None,hist=True,fut_opt="fut")
    get_legacy_fut_opt(market_code=Lumber_code,report_type="opt",hist=True)
    get_legacy_fut_opt(market_code=Lumber_code,report_type="fut",hist=True)

    

    logger.info("Successfully populated data for {}".format(Lumber_code))

except Exception as e:
   
    logger.error("Error in running cod script on {}".format(date.today()))
    logger.error(e)


