from pyspark.sql import functions as F

# ADD LABELS ------------------------------------------------------------------------------
df = spark.read.option("header", False).option("delimiter", "|").csv("s3://ds102-mintchoco-scratch/data/historical_data_2009Q1/historical_data_time_2009Q1.txt")
# df.show()
# df.describe().show() # shows descriptors for columns in df

filtered_df = df.select("_c0", "_c3", "_c8") # important subset of columns
# filtered_df.show()

filtered_df = filtered_df.withColumn(
    "label",
    F.when((F.col("_c3") >= 90) | 
    (F.col("_c8") == "03") | 
    (F.col("_c8") == "06") | 
    (F.col("_c8") == "09"), 1).otherwise(0)
)
# filtered_df.show()

labels_df = df.select("_c0", "label")
# labels_df.show()

# CALCULATE PREDICTED PROBABILITY OF DEFAULT ----------------------------------------------
