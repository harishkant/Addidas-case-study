# Addidas-case-study
Open Library is an initiative of the Internet Archive, a 501(c)(3) non-profit, building a digital library of Internet
sites and other cultural artifacts in digital form. In the section Bulk Data Dumps, they provide public feeds
with the library data.
à https://openlibrary.org/developers/dumps
They also provide a shorter versions of the file for developing or exploratory purposes, where the size is
around 140MB of data instead of ~20GB of the original/full file (referring to the “complete dump”).
à https://s3-eu-west-1.amazonaws.com/csparkdata/ol_cdump.json
Starting with the short version of this file, pls. download it to your local laptop:
wget --continue https://s3-eu-west-1.amazonaws.com/csparkdata/ol_cdump.json -O
/tmp/ol_cdump.json
Note: This is an open exercise, you can use whatever technology you might find useful to showcase your
solution, cloud services, on premise, etc. We suggest to provide also an architectural overview with a
diagram that shows all the components together with a short explanation of each of them

**CASE STUDY**
Please use the JSON file to provide the following information.
1. Load the data
2. Make sure your data set is cleaned enough, so we for example don't include in results with empty/null "titles"
and/or "number of pages" is greater than 20 and "publishing year" is after 1950. State your filters clearly.
3. Run the following queries with the preprocessed/cleaned dataset:
1. Select all "Harry Potter" books
2. Get the book with the most pages
3. Find the Top 5 authors with most written books (assuming author in first position in the array, "key" field and each
row is a different book)
4. Find the Top 5 genres with most books
5. Get the avg. number of pages
6. Per publish year, get the number of authors that published at least one book
4. How would you design a scheduled data pipeline, which would load this data on a daily basis?
Please explain the design principles applied if any (and why).
You have a total of 45 minutes to explain your approach to the case study to the interviewing panel, typically formed by
the Senior Director of Consumer Analytics and his first line.
Final note: this is not about being picture-perfect and presenting glossy marketing slides – ideas, solution quality and
architecture count more than a nice-looking presentation!



**Step 1: Download the file and load in HDFS**

wget --continue https://s3-eu-west-1.amazonaws.com/csparkdata/ol_cdump.json -O /harish/ol_cdump.json

**Load data in HDFS from local**
hadoop fs -copyFromLocal /harish/ol_cdump.json /harish/


**Login into spark shell**
![image](https://user-images.githubusercontent.com/34162166/155930616-a63dc99e-4194-4aea-9de2-6c99a2266f71.png)


**Step2: Profiling the data **
 **create new column author:-**
data = dd.withColumn("author", explode("authors.key"))

**apply Filter title & author is not null:-**
data = data.filter("title is not null and author is not null")

**--get all the row where page is more than 20:-**
data = data.filter("number_of_pages > 20")

**--Filter the data where Titles ending with "=" character or having "+" character:-**
data = data.filter(~col("title").rlike("[=$|\\\+]")).filter(col("publish_date").rlike("2021|20[0-9]{2}|19[0-9]{2}")).withColumn("publish_year",regexp_extract("publish_date","2021|20[0-9]{2}|19[0-9]{2}",0).cast("Integer")).drop("publish_date")

**-- Filter the data Publish year > 1950 records:-**
data = data.filter("publish_year > 1950")

**--Select relevant dataset title,author,publish year,number_of_pages,genres:-**
data = data.select(col("title"), col("author"), col("publish_year"), col("number_of_pages"),col("genres")).distinct()

data.show(5)

![image](https://user-images.githubusercontent.com/34162166/155930189-c10ce376-a27c-4061-9ea3-8f2b771e22ba.png)

**Loading final profiled data in S3**

data.write.save('s3://altanalyticsdwh/casestudy/', format="parquet")

![image](https://user-images.githubusercontent.com/34162166/155938935-2b9671a3-59f0-4395-b604-e02d877aecb3.png)






**CASE STUDY IN PYSPARK**


**1. Select all "Harry Potter" books**

data.filter("title like '%Harry Potter%'").orderBy("publish_year").show(truncate=False)
![image](https://user-images.githubusercontent.com/34162166/155939838-1463dc38-fad0-4384-a88b-05746003da12.png)

**2. Get the book with the most pages**

data.orderBy(col("number_of_pages").desc()).limit(1).show(truncate=False)

![image](https://user-images.githubusercontent.com/34162166/155940037-08d5193d-18f7-4519-b51d-f0d682cb7435.png)

3. Find the Top 5 authors with most written books (assuming author in first position in the array, "key" field and each
row is a different book)

data.groupBy("author").count().orderBy(col("count").desc()).limit(5).show(truncate=False)

![image](https://user-images.githubusercontent.com/34162166/155945123-b7551cee-ab49-4f69-ae50-4612804cb284.png)

4. Find the Top 5 genres with most books

data.withColumn("genre", explode("genres")).withColumn("genre", regexp_replace("genre", "\.$", "")).drop("genres").groupBy("genre").count().orderBy(col("count").desc()).limit(5).show(truncate=False)

![image](https://user-images.githubusercontent.com/34162166/155945515-a1694a77-464d-4f59-8abc-3433e7642d4c.png)


5. Get the avg. number of pages

data.agg(avg("number_of_pages").alias("Average")).show()

![image](https://user-images.githubusercontent.com/34162166/155945800-f233b41c-ea52-4e59-b48a-8d822baf27a7.png)

6. Per publish year, get the number of authors that published at least one book

data.groupBy("publish_year", "author").count().filter("count >=1").drop("count").groupBy("publish_year").count().orderBy(col("publish_year").desc()).show(100, truncate=False)

![image](https://user-images.githubusercontent.com/34162166/155946886-af78a131-0d59-43f9-8718-3fefd2904d15.png)




