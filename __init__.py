import os
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
import pandas as pd
import json

    
# conf = SparkConf() \
#     .setAppName("TgTest")\
#     .config("spark.driver.extraJavaOptions",
#     "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED",)
# sc= SparkContext.getOrCreate(conf=conf)
spark = SparkSession.builder.appName('Test').\
    config("spark.driver.extraJavaOptions",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED").getOrCreate()

## Get the table
professionals_df = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3032/migrasi") \
    .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "merger_loc_emis_siaga") \
    .option("user", "root").option("password", "AE72tHgFbFG09mDj").load()
    
print(professionals_df)


print('success')
