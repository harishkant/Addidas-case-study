
import json
from pyspark.sql.functions import *

"Load data in Dataframe from HDFS location"
dd=spark.read.json("/harish/ol_cdump.json")


"create new column author"
data = dd.withColumn("author", explode("authors.key"))

"apply Filter title & author is not null"
data = data.filter("title is not null and author is not null")

"get all the row where page is more than 20"
data = data.filter("number_of_pages > 20")

"Filter the data where Titles contains special character"
data = data.filter(~col("title").rlike("[=$|\\\+]")).filter(col("publish_date").rlike("2021|20[0-9]{2}|19[0-9]{2}")).withColumn("publish_year",regexp_extract("publish_date","2021|20[0-9]{2}|19[0-9]{2}",0).cast("Integer")).drop("publish_date")

"Filter the data Publish year > 1950 records"
data = data.filter("publish_year > 1950")

"Select relevant dataset title,author,publish year,number_of_pages,genres"
data = data.select(col("title"), col("author"), col("publish_year"), col("number_of_pages"),col("genres")).distinct()


data.show(5)

"Load data from dataframe to AWS S3"

data.write.save('s3://altanalyticsdwh/casestudy/')
