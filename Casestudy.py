from pyspark.sql.functions import *

"1. Select all Harry Potter books"
data.filter("title like '%Harry Potter%'").orderBy("publish_year").show(truncate=False)

"2. Get the book with the most pages"
data.orderBy(col("number_of_pages").desc()).limit(1).show(truncate=False)

"3. Find the Top 5 authors with most written books (assuming author in first position in the array, key field and each row is a different book)"

data.groupBy("author").count().orderBy(col("count").desc()).limit(5).show(truncate=False)

"4. Find the Top 5 genres with most books"

data.withColumn("genre", explode("genres")).withColumn("genre", regexp_replace("genre", ".$", "")).drop("genres").groupBy("genre").count().orderBy(col("count").desc()).limit(5).show(truncate=False)


"5. Get the avg. number of pages"

data.agg(avg("number_of_pages").alias("Average")).show()

"6. Per publish year, get the number of authors that published at least one book"

data.groupBy("publish_year", "author").count().filter("count >=1").drop("count").groupBy("publish_year").count().orderBy(col("publish_year").desc()).show(100, truncate=False)
