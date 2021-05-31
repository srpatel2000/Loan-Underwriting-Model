#### IMPORT STATEMENTS ####
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import s3fs

# import machine learning modules
from sklearn.model_selection import train_test_split # split data into train and test
from sklearn. linear_model import LogisticRegression # apply a logistic regression model
from sklearn.metrics import confusion_matrix # get details on FNs and FPs
from sklearn.metrics import accuracy_score # get the accuracy of the model

# load in the tables
s3 = s3fs.S3FileSystem()
features = pq.ParquetDataset('s3://ds102-mintchoco-scratch/features', filesystem=s3).read_pandas().to_pandas()
features = features.rename(columns={"0": "credit_score", "4": "metro_area", "7": "occupancy_status", "8": "cltv", "9": "dti", \
    "11": "upb", "12": "original_interest_rate", "14": "ppm", "17": "property_type", "19": "loan_sequence_number", \
    "25": "conforming_flag", "28": "harp_indicator"})
labels = pq.ParquetDataset('s3://ds102-mintchoco-scratch/labels/labels.parquet', filesystem=s3).read_pandas().to_pandas()

# join the tables on loan sequence number
merged_df = pd.merge(features, labels, on='loan_sequence_number', how="inner")

# create training a test split
X = merged_df[merged_df.columns.difference(['loan_sequence_number', 'label'])]
y = merged_df[['label']]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.30)

# fit the model
model = LogisticRegression()
model.fit(X_train, y_train)
y_pred = model.predict(X_test)

# display confusion matrix
print(confusion_matrix(y_test, y_pred))

# display accuracy 
print(accuracy_score(y_test, y_pred)