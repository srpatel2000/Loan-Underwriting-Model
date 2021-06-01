# DSC102-PA

## Project Description
This project was created for the SP'21 Iteration of DSC-102: Scalable Analytic at UCSD. In this project, we got to learn how to create a basic loan underwriting model using the [Freddie Mac Single Family Loan](http://www.freddiemac.com/research/datasets/sf_loanlevel_dataset.page) dataset. Although we created a machine learning model, the goal of this project was not about model accuracy, but rather to learn how to make scalable models using AWS, Dask, and Pyspark. Due to this, we spent much time learning how to use these softwares/languages, but spent minimal time finding the best features/models. Below, we will describe how to re-run our project in order to run larger datasets. We'll also be discussing how the project was made scalable. 

## How To Run:

## Feature Engineering and Preprocessing

To preprocess the data, we began filtering the dataset to the following relevent columns.
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

We choose the above relevent column based on ["7 Factors That Affect a Loan Application"]](https://loans.usnews.com/articles/beyond-credit-scores-factors-that-affect-a-loan-application)
