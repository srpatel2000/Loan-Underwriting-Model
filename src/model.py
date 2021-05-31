#### IMPORT STATEMENTS ####
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
from sklearn.model_selection import train_test_split # split data into train and test
from sklearn. linear_model import LogisticRegression # apply a logistic regression model
from sklearn.metrics import confusion_matrix # get details on FNs and FPs
from sklearn.metrics import accuracy_score # get the accuracy of the model

# HOW TO READ IN PARQUET FILES WITH MULTIPLE PARTS

# pq.read_table('part-00000-4a17f992-2966-4cd9-bbea-d9730a693806-c000.snappy.parquet').to_pandas()
# for i in 

# aws s3://ds102-mintchoco-scratch/labels/labels.parquet/
# pd.read_parquet('example_pa.parquet', engine='pyarrow')