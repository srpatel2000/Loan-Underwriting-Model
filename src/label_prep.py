from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import functions as F
import sys

# ASK HOW TO GET DYNAMIC PATH VALUES

def label(path):

    """ This function generates labels from the Delinquency Status and Zero Balance Code of a Specific Loan Sequence Number.
        The label indicates whether a user is considered default or not (1=yes, 0=no). """

    # READ IN DATA ----------------------------------------------------------------------------
    df = spark.read.option("header", False).option("delimiter", "|").csv(path)
    filtered_df = df.select(F.col("_c0").alias("loan_sequence_number"), F.col("_c3").alias("delinquency_status"), F.col("_c8").alias("zero_balance_code")) # important subset of columns

    # ADD LABELS ------------------------------------------------------------------------------
    filtered_df = filtered_df.withColumn(
        "label",
        F.when((F.col("delinquency_status") == "3") | 
        ((F.col("zero_balance_code") == "03") | 
        (F.col("zero_balance_code") == "06") | 
        (F.col("zero_balance_code") == "09")), 1).otherwise(0)
    )
    labels_df = filtered_df.select("loan_sequence_number", "label")

    # CALCULATE PREDICTED PROBABILITY OF DEFAULT ----------------------------------------------
    avg_df = labels_df.groupby("loan_sequence_number").agg({'label': 'mean'})

    # ADD LABELS BASED ON PROBABILITY ---------------------------------------------------------
    labeled_df = avg_df.withColumn(
    "label",
    F.when((F.col("avg(label)") >= 0.5), 1).otherwise(0)
    )

    # SELECT RELEVANT COLUMNS -----------------------------------------------------------------
    labeled_df = labeled_df.select("loan_sequence_number", "label")
    return labeled_df

if __name__ == '__main__':
    spark = SparkSession.builder.appName("label_prep").master("local").getOrCreate()
    print(str(sys.argv))
    label_dfs = label("s3://ds102-mintchoco-scratch/data/historical_data_2009Q1/historical_data_time_2009Q1.txt")
    label_dfs.write.format("parquet").mode("overwrite").save("s3://ds102-mintchoco-scratch/labels/labels.parquet")
