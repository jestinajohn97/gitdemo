// Databricks notebook source
// /FileStore/shared_uploads/jestinajohn1997@gmail.com/Weblog.csv

// COMMAND ----------

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


// COMMAND ----------


// Disable Logs
Logger.getLogger("org").setLevel(Level.OFF)
val spark = SparkSession.builder().appName("WebLog").master("local[*]").getOrCreate()

// COMMAND ----------

import spark.implicits._

// COMMAND ----------

val logs_DF = spark.read.text("dbfs:/FileStore/shared_uploads/jestinajohn1997@gmail.com/Weblog.csv")

val header = logs_DF.first() // Extract Header
val logs_DF1 = logs_DF.filter(row => row != header)


logs_DF1.printSchema()

// COMMAND ----------

logs_DF1.show(5,false)

// COMMAND ----------

// MAGIC %md
// MAGIC ### a)	Parsing the Log Files using RegExp & Pre-process Raw Log Data into Data frame with attributes. 

// COMMAND ----------

val hosts = logs_DF.select(regexp_extract($"value","""([^(\s|,)]+)""", 1).alias("Host"))
hosts.show()

// COMMAND ----------

//timestamp

val timestamp = logs_DF1.select(regexp_extract($"value", """\S(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})""", 1).alias("Timestamp"))
timestamp.show(false)

// COMMAND ----------

//request url
val url = logs_DF1.select(regexp_extract($"value", """(\S+)\s(\S+)\s*(\S*)""", 2).alias("Request_URL"))
url.show()


// COMMAND ----------

// Extract HTTP Method
val method = logs_DF1.select(regexp_extract($"value", """(\w+)\s(\S+)\s(\S+)""", 1).alias("HTTP_Method"))
method.show()

// COMMAND ----------

// Protocol
val protocol = logs_DF1.select(regexp_extract($"value", """(\S+)\s(\S+)\s(\S+)(,)""", 3).alias("Protocol"))
protocol.show()

// COMMAND ----------

 val status = logs_DF1.select(regexp_extract($"value", """\,(\d{3})""", 1).alias("STATUS"))
status.show()

// COMMAND ----------

// Merge multiple regular expressions
val weblog_df = logs_DF1.select(regexp_extract($"value","""([^(\s|,)]+)""", 1).alias("Host"),
                           regexp_extract($"value", """\S(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})""", 1).alias("Timestamp"),
                           regexp_extract($"value", """(\w+)\s(\S+)\s(\S+)""",1).alias("HTTP_Method"),
                           regexp_extract($"value", """(\S+)\s(\S+)\s*(\S*)""", 2).alias("Request_URL"),
                           regexp_extract($"value","""(\S+)\s(\S+)\s(\S+)(,)""", 3).alias("HTTP Protocol"),
                           regexp_extract($"value", """,(\d{3})""", 1).alias("Status"))
weblog_df.show(20)

weblog_df.printSchema()


// COMMAND ----------

// MAGIC %md
// MAGIC ###  b) Find Count of Null, None, NaN of all dataframe columns

// COMMAND ----------


import org.apache.spark.sql.functions.{col,when,count}
import org.apache.spark.sql.Column

// UDF
def countNullCols (columns:Array[String]):Array[Column] = {
   columns.map(c => {
   count(when(col(c).isNull, c)).alias(c)
  })
}

weblog_df.select(countNullCols(weblog_df.columns): _*).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### c) Pre-process and fix timestamp month name to month value. Convert Datetime (timestamp column) as Days, Month & Year.

// COMMAND ----------


weblog_df.select(to_date($"Timestamp")).show(5)


// COMMAND ----------

val month_map = Map("Jan" -> 1, "Feb" -> 2, "Mar" -> 3, "Apr" -> 4, "May" -> 5, "Jun" -> 6, "Jul" -> 7, "Aug" -> 8, "Sep" -> 9,
                   "Oct" -> 10, "Nov" -> 11, "Dec" -> 12)
// UDF 
def parse_time(s : String):String = {
  "%3$s-%2$s-%1$s %4$s:%5$s:%6$s".format(s.substring(0,2), month_map(s.substring(3,6)), s.substring(7,11), 
                                             s.substring(12,14), s.substring(15,17), s.substring(18))
}

val toTimestamp = udf[String, String](parse_time(_))

val logsDF = weblog_df.select($"*", to_timestamp(toTimestamp($"Timestamp")).alias("time")).drop("Timestamp")
logsDF.show()


// COMMAND ----------

val time_df = logsDF.withColumn("Day",dayofmonth($"time")).withColumn("Month",month($"time")).withColumn("Year",year($"time")).drop("time")                 
time_df.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### d) Convert Textfile Format to Parquet File Format

// COMMAND ----------

//weblog_df.write.parquet("dbfs:/FileStore/shared_uploads/jestinajohn1997@gmail.com/weblogg/")

// COMMAND ----------

// Read Parquet File Format
val parquetLogs = spark.read.parquet("dbfs:/FileStore/shared_uploads/jestinajohn1997@gmail.com/weblogg/")
parquetLogs.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### e) Show the summary of each column. 

// COMMAND ----------


// parquetLogs.describe(cols="Status").show()
parquetLogs.summary().show()

// COMMAND ----------

val time_df = logsDF.withColumn("Day",dayofmonth($"time")).withColumn("Month",month($"time")).withColumn("Year",year($"time")).drop("time")                 
time_df.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### f)	Display frequency of 200 status code in the response for each month. 

// COMMAND ----------

time_df.filter($"Status"===200).groupBy("Month").count().sort(desc("count")).show(false)


// COMMAND ----------

// MAGIC %md
// MAGIC ### g)	Frequency of Host Visits in November Month.

// COMMAND ----------


//time_df.filter($"Month"===11).groupBy("Host").count().sort(desc("count")).show(false)


// COMMAND ----------

// MAGIC %md
// MAGIC ###  h)	Display Top 15 Error Paths - status != 200.

// COMMAND ----------

parquetLogs.filter($"Status" =!= 200).groupBy("Request_URL").count().sort(desc("count")).show(15)


// COMMAND ----------

// MAGIC %md
// MAGIC ### i)	Display Top 10 Paths with Error - with status equals 200.

// COMMAND ----------

parquetLogs.filter($"Status" === 200).groupBy("Request_URL").count().sort(desc("count")).show(10)


// COMMAND ----------

// MAGIC %md
// MAGIC ### j)	Exploring 404 status code. Listing 404 status Code Records. List Top 20 Host with 404 response status code (Query + Visualization).

// COMMAND ----------

time_df.createOrReplaceTempView("weblogsTable")
//spark.sql("select * from weblogsTable limit 10").show()

//spark.sql("select Status, count(*) as Count from weblogsTable where Status = 404 group by Status").show()

// COMMAND ----------

// MAGIC %sql
// MAGIC select host, count(*) as Count from weblogsTable where Status = 404 group by host order by Count Desc limit 20

// COMMAND ----------

// MAGIC %md
// MAGIC ### k)	Display the List of 404 Error Response Status Code per Day (Query + Visualization).

// COMMAND ----------

// MAGIC %sql
// MAGIC select day, count(*) as Count from weblogsTable where Status = 404 group by  day order by day desc limit 30

// COMMAND ----------

// MAGIC %md
// MAGIC ### l)	List Top 20 Paths (Endpoint) with 404 Response Status Code.

// COMMAND ----------

spark.sql("select Request_URL, count(*) as Count from weblogsTable where Status = 404 group by Request_URL order by Count Desc limit 20").show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ### m)	Query to Display Distinct Path responding 404 in status error.

// COMMAND ----------

spark.sql("select distinct(Request_URL) from weblogsTable where Status = 404").show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ### n)	Find the number of unique source IPs that have made requests to the webserver for each month.

// COMMAND ----------

 spark.sql("select distinct(host) from weblogsTable").show(false)


// COMMAND ----------

// MAGIC %md
// MAGIC ### o)Display the top 20 requested Paths in each Month (Query + Visualization).

// COMMAND ----------

// spark.sql("select Month,count(Request_URL) from weblogsTable").show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ### p) Query to Display Distinct Path responding 404 in status error.

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct(Request_URL) from weblogsTable where Status = 404

// COMMAND ----------


