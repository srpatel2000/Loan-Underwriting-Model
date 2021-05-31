# import statements
import dask.dataframe as dd
from distributed import Client
from dask_ml import preprocessing
import dask.array as da
import numpy as np
import sys

def replace_null(data, col, val):
    return data[col].where(data[col] == 'Y', val)

def valid_data(data, low, high, col):
    return data[(data[col] <= high) & (data[col] >= low)]

def rename_col(data):
    new_columns = []
    for col in data.columns:
        new_columns.append(str(col))
    return data.rename(columns=dict(zip(data.columns, new_columns)))

def encoding(data, col):
    le = preprocessing.LabelEncoder()
    return dd.from_dask_array(le.fit_transform(data[col]), index=data.index)


def clean_features(path):
    og_data_df = dd.read_csv(path, sep="|", header=None, dtype={25:str, 27: object})
    # monthly_perf_data_df = dd.read_csv("historical_data_time_2009Q1.txt", sep="|", header=None, dtype={24: 'object', 28: 'object',29: 'object',3: 'object'})
    og_data_df = rename_col(og_data_df)

    #### PREPROCESSING ####

    # CLEANING ORIGINATION DATA FILE --------------------------------------------------------------------------------------

    # filter relevant column
    cleaned_df = og_data_df[['0', '4', '7', '8', '9', '11', '12', '14', '17', '19', '25', '28']]

    # removing rows with invalid credit scores 
    cleaned_df = valid_data(cleaned_df, 301, 850, '0')
    # removing rows with invalid debt to income ratio 
    cleaned_df = valid_data(cleaned_df, 0, 65, '9')

    # fill null values
    cleaned_df['25'] = replace_null(cleaned_df, '25', 'N')
    cleaned_df['28'] = replace_null(cleaned_df, '28', 'N')

    #### FEATURE ENGINEERING ####

    # Feature #1: using MSA to create column that 
    cleaned_df['4'] = cleaned_df['4'].where(cleaned_df['4'] > 0, 0) # indicates that mortgage property is not in MSA
    cleaned_df['4'] = cleaned_df['4'].where(cleaned_df['4'] == 0, 1)

    print(cleaned_df.compute())

    # LABEL ENCODING CATEGORICAL VARIABLES --------------------------------------------------------------------------------------

    cleaned_df['7'] = encoding(cleaned_df, '7')
    cleaned_df['14'] = encoding(cleaned_df, '14')
    cleaned_df['17'] = encoding(cleaned_df, '17')
    cleaned_df['25'] = encoding(cleaned_df, '25')
    cleaned_df['28'] = encoding(cleaned_df, '28')

    print('final')
    print('\n', cleaned_df.compute())

    return cleaned_df

if __name__ == '__main__':
    client = Client()
    # clean_features("historical_data_2009Q1.txt")
    features_df = clean_features(sys.argv[1])
    features_df.to_parquet(sys.argv[2])
