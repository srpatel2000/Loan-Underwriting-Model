# IMPORT STATEMENTS ------------------------------------------------------------------------------------
import dask.dataframe as dd
from distributed import Client
from dask_ml import preprocessing
import dask.array as da
import numpy as np
import sys

### HELPER FUNCTIONS ###
def replace_null(data, col, val):
    """
    Replaces null values in specific column

    Parameters
    ----------
    data : Dataframe
    col : str
        the name of column with null values
    val : str
        the value that will replace the null data

    Returns 
    ----------
    Updated dataframe
    
    """
    return data[col].where(data[col] == 'Y', val)

def valid_data(data, low, high, col):
    """
    Drops rows that include invalid data inputs

    Parameters
    ----------
    data : Dataframe
    low : int
        low threshold of valid in input
    high : int
        high threshold of valid in input
    col : str
        the name of column with null values
    
    Returns 
    ----------
    Updated dataframe
    """
    return data[(data[col] <= high) & (data[col] >= low)]

def rename_col(data):
    """
    Replace integer column names with string

    Parameters
    ----------
    data : Dataframe

    Returns 
    ----------
    Updated dataframe
    
    """
    new_columns = []
    for col in data.columns:
        new_columns.append(str(col))
    return data.rename(columns=dict(zip(data.columns, new_columns)))

def encoding(data, col):
    """
    Encode categorical column with value between 0 and n_classes-1 

    Parameters
    ----------
    data : Dataframe
    col : str
        the name of column with null values

    Returns 
    ----------
    Transformed column
    
    """
    le = preprocessing.LabelEncoder() # dask-ml label encoder
    return dd.from_dask_array(le.fit_transform(data[col]), index=data.index)


def clean_features(path):
    """
    This function preprocesses the data and generates relevent features from the origination dataset.

    Parameters
    ----------
    path : str
        path to the origination dataset
    
    Returns 
    ----------
    Updated dataframe

    """
    og_data_df = dd.read_csv(path, sep="|", header=None, dtype={25:str, 27: object}) # read in the file
    og_data_df = rename_col(og_data_df) # rename int col to string

    #### PREPROCESSING ####
    # CLEANING ORIGINATION DATA FILE -------------------------------------------------------------------
    # filter relevant columns
    cleaned_df = og_data_df[['0', '4', '7', '8', '9', '11', '12', '14', '17', '19', '25', '28']]

    # removing rows with invalid inputs 
    cleaned_df = valid_data(cleaned_df, 301, 850, '0') # credit scores
    cleaned_df = valid_data(cleaned_df, 0, 65, '9') # debt to income ratio

    # fill null values
    cleaned_df['25'] = replace_null(cleaned_df, '25', 'N') # super conforming flag
    cleaned_df['28'] = replace_null(cleaned_df, '28', 'N') # harp indicator
    # END OF CLEANING ORIGINATION DATA FILE ------------------------------------------------------------

    #### FEATURE ENGINEERING ####
    # Feature #1: using MSA to indicate whether mortage property is in a metropolitan area
    cleaned_df['4'] = cleaned_df['4'].where(cleaned_df['4'] > 0, 0) # not in metropolitan area
    cleaned_df['4'] = cleaned_df['4'].where(cleaned_df['4'] == 0, 1) # in metropolitan area

    # LABEL ENCODING CATEGORICAL VARIABLES -------------------------------------------------------------
    cleaned_df['7'] = encoding(cleaned_df, '7')
    cleaned_df['14'] = encoding(cleaned_df, '14')
    cleaned_df['17'] = encoding(cleaned_df, '17')
    cleaned_df['25'] = encoding(cleaned_df, '25')
    cleaned_df['28'] = encoding(cleaned_df, '28')
    # END OF LABEL ENCODING CATEGORICAL VARIABLES ------------------------------------------------------

    return cleaned_df

if __name__ == '__main__':
    client = Client()
    features_df = clean_features(sys.argv[1])
    features_df.to_parquet(sys.argv[2])


