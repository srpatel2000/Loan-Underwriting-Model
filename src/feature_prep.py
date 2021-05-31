# import statements
import dask.dataframe as dd
from distributed import Client
from dask_ml import preprocessing
import dask.array as da
import numpy as np

#client = Client()

# loading in datatable with adjusted datatypes
# og_data_df = dd.read_csv("historical_data_2009Q1.txt", sep="|", header=None, dtype={25:str, 27: object})
# monthly_perf_data_df = dd.read_csv("historical_data_time_2009Q1.txt", sep="|", header=None, dtype={24: 'object', 28: 'object',29: 'object',3: 'object'})

# checking if dataframe was loaded in correctly
# og_data_df.compute()
# monthly_perf_data_df.compute()

#### PREPROCESSING ####
# CLEANING ORIGINATION DATA FILE --------------------------------------------------------------------------------------

# number of rows of original df: 0 621,539 
# print(len(og_data_df))

#new_columns = []
#for col in og_data_df.columns:
#    new_columns.append(str(col))
#og_data_df = og_data_df.rename(columns=dict(zip(og_data_df.columns, new_columns)))

# START : remove or fix null values ----------
# removing rows with invalid credit scores 
# cleaned_df = og_data_df[(og_data_df['0'] <= 850) & (og_data_df['0'] >= 301)]
# removing rows with invalid debt to income ratio 
# cleaned_df = og_data_df[(og_data_df['9'] <= 65) & (og_data_df['9'] > 0)] # rows: 618,394

# number of null values columns
# print(og_data_df.isna().sum(axis=0).compute())

# Pre-Harp Loan Sequence Number (#26) --> has too many nulls & isn't important 

# filled columns with many null values with 'N 
# cleaned_df['25'] = cleaned_df['25'].where(cleaned_df['25'] == 'Y','N')
# cleaned_df['28'] = cleaned_df['28'].where(cleaned_df['28'] == 'Y','N')
# END : remove or fix null values ------------

# check variance of numerical columns
# for col in cleaned_df.columns:
#     if cleaned_df[col].dtype == int or cleaned_df[col].dtype == float:
#         print(col)
#         variance = cleaned_df[col].values.var().compute()
#         print(cleaned_df[col].values.var().compute())
#         print("\n")
#         if variance == 0: 
#             cleaned_df = cleaned_df.drop(col, axis=1) 
# remove columns with a very low variance

# filter in relevant columns
# cleaned_df = cleaned_df[['0', '4', '7', '8', '9', '11', '12', '14', '17', '19', '25', '28']]

# cleaned_df = cleaned_df[cleaned_df['7'] != 9] 

# CLEANING MONTHLY PERFORMANCE DATA FILE -------------------------------------------------------------------------------
# print(monthly_perf_data_df.isna().sum(axis=0).compute())

#### FEATURE ENGINEERING ####
# CLEANING ORIGINATION DATA FILE --------------------------------------------------------------------------------------

# dummy encoding of Metropolitan Statistical Area
#cleaned_df['4'] = cleaned_df['4'].where(cleaned_df['4'] > 0, 0) # indicates that mortgage property is not in MSA
#cleaned_df['4'] = cleaned_df['4'].where(cleaned_df['4'] == 0, 1) # indicates that mortgage property is in MSA

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

    print(cleaned_df.head())

    # LABEL ENCODING CATEGORICAL VARIABLES --------------------------------------------------------------------------------------

    cleaned_df['7'] = encoding(cleaned_df, '7')
    cleaned_df['14'] = encoding(cleaned_df, '14')
    cleaned_df['17'] = encoding(cleaned_df, '17')
    cleaned_df['25'] = encoding(cleaned_df, '25')
    cleaned_df['28'] = encoding(cleaned_df, '28')

    print('final')
    print('\n', cleaned_df.head())


    #result.to_parquet()


if __name__ == '__main__':
    client = Client()
    clean_features("historical_data_2009Q1.txt")


# Label Encoder
# from dask_ml import preprocessing
# cat_df = cleaned_df.astype('category')
# cat_df = cat_df.categorize()

# encoder = preprocessing.LabelEncoder() --> for OneHotEncoding preprocessing.OneHotEncoder(sparse=False)
# encoder.fit(cat_df)
# encoder.transform(cat_df).compute()
# data = dd.from_pandas(pd.Series(['a', 'a', 'b'], dtype='category'), npartitions=2)

# le = preprocessing.LabelEncoder()
# enc = preprocessing.OneHotEncoder()


# cat_7 = dd.from_dask_array(le.fit_transform(cleaned_df['7']), index=cleaned_df.index)

# cat columns: 7, 14, 17, 25, 28

# check if removing values from 8/11 cltv (999)