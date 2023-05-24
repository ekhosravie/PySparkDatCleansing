from pyspark.sql.functions import *
from pyspark.sql.types import *

# Load data into a DataFrame
df = spark.read.csv("path/to/data.csv", header=True)

# Remove duplicates
df = df.dropDuplicates()

# Remove null values
df = df.na.drop()

# Remove leading/trailing whitespaces
df = df.select([trim(col(c)).alias(c) for c in df.columns])

# Convert columns to the correct data types
df = df.withColumn("age", df["age"].cast(IntegerType()))
df = df.withColumn("salary", df["salary"].cast(DoubleType()))

# Remove special characters from columns
df = df.withColumn("name", regexp_replace(col("name"), "[^a-zA-Z\\s]", ""))

# Remove stop words from text columns
stop_words = StopWordsRemover.loadDefaultStopWords("english")
stop_words_remover = StopWordsRemover(inputCol="text", outputCol="text_cleaned", stopWords=stop_words)
df = stop_words_remover.transform(df).drop("text").withColumnRenamed("text_cleaned", "text")