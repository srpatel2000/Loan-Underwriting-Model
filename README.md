# DSC102-PA

## Project Description
This project was created for the SP'21 Iteration of DSC-102: Scalable Analytic at UCSD. In this project, we got to learn how to create a basic loan underwriting model using the [Freddie Mac Single Family Loan](http://www.freddiemac.com/research/datasets/sf_loanlevel_dataset.page) dataset. Although we created a machine learning model, the goal of this project was not about model accuracy, but rather to learn how to make scalable models using AWS, Dask, and Pyspark. Due to this, we spent much time learning how to use these softwares/languages, but spent minimal time finding the best features/models. Below, we will describe how to re-run our project in order to run larger datasets. We'll also be discussing how the project was made scalable. 

## How To Run:

* __feature_prep.py__: This file was run on EC2 instance. In order to initialize the EC2 instance, you can use: src/emr_initialization.sh file as a guide. Once SSH'd into the instance, you can run this line of code: 

    *     python feature_prep.py <INPUT DATA PATH> <OUTPUT FILE PATH>
  
  EXAMPLE:
    *     python feature_prep.py s3://ds102-mintchoco-scratch/data/historical_data_2009Q1/historical_data_2009Q1.txt s3://ds102-mintchoco-scratch/features


* __label_prep.py__: This file was run on the master node of an EMR cluster. In order to initialize the EMR cluster, you can use: src/emr_initialization.sh file as a guide. Once SSH'd into the cluster, you can run this line of code: 

    *     spark-submit --deploy-mode cluster s3://ds102-mintchoco-scratch/label_prep.py <INPUT DATA PATH> <OUTPUT FILE PATH>
  
  EXAMPLE:
    *     spark-submit --deploy-mode cluster s3://ds102-mintchoco-scratch/label_prep.py s3://ds102-mintchoco-scratch/data/historical_data_2009Q1/historical_data_time_2009Q1.txt s3://ds102-mintchoco-scratch/labels/labels.parquet/

## Feature Engineering and Preprocessing

To preprocess the data, we began filtering the Origination dataset to the following relevent columns.
* Credit Score
* Metropolitan Statistical Area
* Number of Units
* Occupancy Status
* Original Combined Loan-To-Value 
* Original Debt-To-Income (DTI) Ratio
* Original Loan-To-Value (LTV) 
* Original Interest Rate
* Prepayment Penalty Mortgage (PPM) Flag
* Property Type 
* Loan Sequence Number (needed for merging with monthly performance dataset)
* Super Conforming Flag
* Harp Indicator

We choose the above relevent column based on ["7 Factors That Affect a Loan Application"](https://loans.usnews.com/articles/beyond-credit-scores-factors-that-affect-a-loan-application) as well as advice from loan-takers. 
   
In regards to cleaning our data, we chose to drop invalid inputs for credit score and debt-to-income ratio since both are highly significant features. For feature engineering, we generated a column called metro_area using Metropolitan Statistical Area column which indicated whether mortage property lies in a metropolitan area {0: not in metropolitan area, 1: in metropolitan area}. We also decided to use label (one-hot) encoder to handle categorical data.
   
## Label Preparation
   
In order to attain the labels, we looked at the Monthly Performance dataset and defined a loan-taker as default at 90 days or more delinquent. In addition to delinquency status, we used their Zero Balance Code. If this code was listed as "03", "06", or "09", the loan was also be considered a default.
