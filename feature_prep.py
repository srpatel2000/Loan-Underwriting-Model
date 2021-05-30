
# import statements
import dask.dataframe as dd

from distributed import Client
client = Client()

# loading in datatable with adjusted datatypes
df = dd.read_csv("historical_data_2009Q1.txt", sep="|", header=None, dtype={4: int, 25:str, 27: object} )

# checking if dataframe was loaded in correctly

# number of rows of original df: 0 621539 
total_rows = len(df)

#### PREPROCESSING ####

# removing rows with invalid credit scores 
cleaned_df = df[(df[0] <= 850) & (df[0] >= 301)]
new_total = len(cleaned_df) # resulting rows: 621494 

# number of null valies in MSA (col 4) -> 126286
num_null = df.isna().sum(axis=0).compute()

# drop column: Pre-Harp Loan Sequence Number (#26)
cleaned_df = cleaned_df.drop(26,axis=1)
