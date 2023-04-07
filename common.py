
import os
from pathlib import Path
import pandas as pd
from backend.db.dbconnect import connect_to_database
PDFS=[]


#ignore list

IGNORE_LIST=["LT1601.pdf",
"LT1602.pdf",
"LT1603.pdf",
"LT1604.pdf"
"LT1605 revised v2.pdf",
"LT1605 revised (2).pdf",
"LT1605.pdf",
"LT1702.pdf",
"LT2001Prelim.pdf",
"LT2002Prelim.pdf",
"LT2003Prelim.pdf",
"LT2004Prelim.pdf",
]


def check_if_exists(filepath: str, checkparm: str):
    """
    Check if a file exists at the specified `filepath` and whether a specific 
    value `checkparm` is present in the "File" column of a CSV file at that `filepath`.
    
    Args:
        filepath: A string representing the file path to check for file existence and CSV file.
        checkparm: A string representing the value to check for in the "File" column of the CSV file.
    
    Returns:
        A boolean value representing whether the file exists at the specified `filepath` and 
        whether the specific `checkparm` value is present in the "File" column of the CSV file.
    """
    # Construct the file path
    filepath = (Path(__file__).resolve().parent).joinpath(filepath)
    
    # Check if the file exists at the filepath
    check = os.path.exists(filepath)
    
    if check:
        # Read the CSV file at the filepath
        df = pd.read_csv(filepath)
        files = df["File"].values

        # Check if the checkparm value is present in the "File" column of the CSV file
        if checkparm in files:
            return True
        else :
            return False
    else:
        return False


def save_to_dir(df, directory, filename, mapper=None, sub_dir=None):
    """
    Save a pandas DataFrame to a specified directory and file name.

    Args:
        df (pandas.DataFrame): The DataFrame to be saved.
        directory (str): The name of the directory where the file will be saved.
        filename (str): The name of the file to be saved.
        mapper (Mapper class): Optional mapper class to map the DataFrame to a database table.
        sub_dir (str): Optional subdirectory name to be created within the main directory.

    Returns:
        None
    """
    # set the file path and create the directory if it doesn't exist
    path = (Path(__file__).resolve().parent).joinpath(directory)
    if sub_dir:
        path = (Path(__file__).resolve().parent).joinpath(directory).joinpath(sub_dir)
    if not os.path.exists(path):
        os.makedirs(path)

    filepath = os.path.join(path, filename)

    # create or append data to the file
    if not os.path.isfile(str(filepath)):
        df.to_csv(str(filepath), header='column_names')
    else:
        df.to_csv(str(filepath), mode='a', header=False)

    # if a mapper is provided, insert the data into the database
    if mapper:
        insert_into_database(mapper, df.to_dict(orient="records"))

    

def insert_into_database(mapper, data):
    """
    Wrapper function to insert or update data to the database

    Parameters:
    mapper (class): A mapper class representing the database table to be updated
    data (list): A list of dictionaries containing the data to be updated or inserted

    Returns:
    None
    """

    # Connect to the database
    db_engine = connect_to_database()
    ssn = db_engine()

    try:
        # Try to bulk insert the data
        ssn.bulk_insert_mappings(mapper, data)
        ssn.commit()
        print("Records added to the database")

    except Exception as e:
        # Rollback the transaction if there was an error
        ssn.rollback()

        try:
            # If insertion fails, try to update existing data
            ssn.bulk_update_mappings(mapper, data)
            ssn.commit()
            print("Records updated in the database")

        except Exception as e:
            # If update also fails, print the error message
            print("Error adding or updating data to the database: {}".format(e))

    finally:
        # Close the session
        ssn.close()
