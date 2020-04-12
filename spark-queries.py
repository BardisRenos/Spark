import pandas as pd
from pyspark.sql.functions import *
import datetime
import json
from pyspark.sql import Window, functions
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as F


sc = SparkSession.builder.appName("PysparkExample")\
    .config("spark.sql.shuffle.partitions", "50")\
    .config("spark.driver.maxResultSize", "5g")\
    .config("spark.sql.execution.arrow.enabled", "true")\
    .getOrCreate()

# JSON
dataframe = sc.read.json('nyt2.json')


# dataframe.createGlobalTempView("books")
# print(sc.sql("select title from global_temp.books where author = 'Debbie Macomber' and publisher = 'Mira' and title <> 'TWENTY WISHES'").show())


# print(dataframe.select(dataframe.title, dataframe.author, dataframe.publisher).filter(dataframe.author == 'Debbie Macomber').filter(dataframe.publisher == 'Mira').filter(dataframe.title != 'TWENTY WISHES').count())

# print(dataframe.select(dataframe.title, dataframe.author, when(dataframe.author == 'Debbie Macomber', 1).otherwise(0)).show())

# print(dataframe.filter(dataframe.author == 'Debbie Macomber').filter(dataframe.publisher == 'Mira').filter(dataframe.title != 'TWENTY WISHES').count())

# print("The result is: ", dataframe.where(dataframe.author == 'Debbie Macomber').where(dataframe.title== 'TWENTY WISHES').count())

# print(dataframe.dtypes)
# print(dataframe.filter(dataframe.author == "Dean R Koontz").show())
# print(dataframe.where(dataframe.author == "Dean R Koontz").count())
# print(dataframe.describe(['author']).show())
# print(dataframe.distinct().count(), dataframe.count(), dataframe.dropDuplicates().count())
# print(dataframe.count(), len(dataframe.columns))
# print(dataframe.columns)
# print(dataframe.printSchema())
# print(dataframe.show())
# print(dataframe.select("author").show())
# print(dataframe.select("author", "price").show(5))
# print(dataframe.select(dataframe["author"], dataframe["price"]).show(5))
# print(dataframe.filter(dataframe["title"]).show(5))

sc.stop()
