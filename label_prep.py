from pyspark.sql import functions as F

df = spark.read.option("header", False).option("delimiter", "|").csv("s3://ds102-mintchoco-scratch/data/historical_data_2009Q1/historical_data_time_2009Q1.txt")
# df.show()
# df.describe().show() # shows descriptors for columns in df

filtered_df = df.select("_c0", "_c3", "_c8") # important subset of columns
# filtered_df.show()

# ADD LABELS ------------------------------------------------------------------------------
filtered_df = filtered_df.withColumn(
    "label",
    F.when((F.col("_c3") >= 90) | 
    (F.col("_c8") == "03") | 
    (F.col("_c8") == "06") | 
    (F.col("_c8") == "09"), 1).otherwise(0)
)
# filtered_df.show()

labels_df = filtered_df.select("_c0", "label")
# labels_df.show()

# CALCULATE PREDICTED PROBABILITY OF DEFAULT ----------------------------------------------
avg_df = labels_df.groupby("_c0").agg({'label': 'mean'})

# ADD LABELS BASED ON PROBABILITY ---------------------------------------------------------
labeled_df = avg_df.withColumn(
    "label",
    F.when((F.col("avg(label)") >= 0.5), 1).otherwise(0)
)

# SAVE DF INTO PARQUET TABLE --------------------------------------------------------------
labeled_df = labeled_df.select("_c0", "label")
labeled_df.write.format("parquet").mode("overwrite").save("s3://ds102-mintchoco-scratch/labels/labels.parquet")