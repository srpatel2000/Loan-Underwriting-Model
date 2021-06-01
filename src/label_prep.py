from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

# IS THIS THE CORRECT WAY OF READING IN DYNAMIC FILES? --> include readme on how to run scripts
# spark-submit --deploy-mode cluster s3://ds102-mintchoco-scratch/label_prep.py

def label(path):

    """ This function generates labels from the Delinquency Status and Zero Balance Code of a Specific Loan Sequence Number.
        The label indicates whether a user is considered default or not (1=yes, 0=no). """

    # READ IN DATA ----------------------------------------------------------------------------
    df = spark.read.option("header", False).option("delimiter", "|").csv(path)
    filtered_df = df.select(F.col("_c0").alias("loan_sequence_number"), F.col("_c3").alias("delinquency_status"), F.col("_c8").alias("zero_balance_code")) # important subset of columns

    # ADD LABELS ------------------------------------------------------------------------------
    filtered_df = filtered_df.withColumn(
        "label",
        F.when(
        (   
            (F.col("delinquency_status") != "XX") & 
            (F.col("delinquency_status") != "0") &
            (F.col("delinquency_status") != "1") &
            (F.col("delinquency_status") != "2") &
            (F.col("delinquency_status") != "R") &
            (F.col("delinquency_status") != " ")
        ) | (  
            (F.col("zero_balance_code") == "03") |
            (F.col("zero_balance_code") == "06") | 
            (F.col("zero_balance_code") == "09")
        ), 1).otherwise(0)
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
    spark = SparkSession.builder.getOrCreate()
    label_dfs = label(sys.argv[1])
    label_dfs.write.format("parquet").mode("overwrite").save(sys.argv[2])
