#!/usr/bin/env python
# coding: utf-8

import pyspark
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types

schema = types.StructType([
    types.StructField('Wave', types.StringType(), True), 
    types.StructField('SiteID', types.StringType(), True), 
    types.StructField('Date', types.StringType(), True), 
    types.StructField('Weather', types.StringType(), True), 
    types.StructField('Time', types.StringType(), True), 
    types.StructField('Day', types.StringType(), True), 
    types.StructField('Round', types.StringType(), True), 
    types.StructField('Direction', types.StringType(), True), 
    types.StructField('Path', types.StringType(), True), 
    types.StructField('Mode', types.StringType(), True), 
    types.StructField('Count', types.LongType(), True)
])

parser = argparse.ArgumentParser()

parser.add_argument('--input_2023', required=True)
parser.add_argument('--input_2024', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_2023 = args.input_2023
input_2024 = args.input_2024
output = args.output


spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'dataproc-temp-us-central1-300103389332-g2wqcss3')

df_2023 = spark.read.csv(input_2023, header=True, schema=schema)

df_2023 = df_2023.withColumn('Year', F.lit('2023'))

df_2024 = spark.read.csv(input_2024, header=True, schema=schema)

df_2024 = df_2024.withColumn('Year', F.lit('2024'))

df_all = df_2023.unionAll(df_2024)

df_all.createOrReplaceTempView("cycling_data")

df_result = spark.sql('''
SELECT
    Year,
    SiteID,
    Weather,
    Direction,
    Path,
    Mode,
    SUM(Count) AS Total_Count,  -- Total traffic count
    AVG(Count) AS Avg_Count,    -- Average traffic count per record
    MAX(Count) AS Max_Count,    -- Maximum traffic count recorded
    MIN(Count) AS Min_Count     -- Minimum traffic count recorded
FROM cycling_data
GROUP BY Year, SiteID, Weather, Direction, Path, Mode
ORDER BY Year, SiteID
''')

df_result.write \
        .format("com.google.cloud.spark.bigquery.v2.Spark34BigQueryTableProvider") \
        .option("table", output) \
        .option("temporaryGcsBucket", "dataproc-temp-us-central1-300103389332-g2wqcss3") \
        .save()
    


