# Databricks notebook source
# MAGIC %md
# MAGIC #Configuration

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "73095a70-cebd-46d2-8769-b08db18b31b1",
"fs.azure.account.oauth2.client.secret": '9MC8Q~MwZ734hn~wAxUOW_.kHGu6iD1k4_IAYbqK',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/f8e3498d-d062-493b-aa0a-bf30a095e647/oauth2/token"}

dbutils.fs.mount(
source = "abfss://de-project-data@deprojectsonbao.dfs.core.windows.net/",
mount_point = "/mnt/tokyoolympic",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/tokyoolympic

# COMMAND ----------

# MAGIC %md
# MAGIC #Import Data

# COMMAND ----------

athletes = spark.read.format('csv').option('header', 'true').load('/mnt/tokyoolympic/raw-data/Athletes.csv')
coaches = spark.read.format('csv').option('header', 'true').load('/mnt/tokyoolympic/raw-data/Coaches.csv')
gender = spark.read.format("csv").option("header","true").load("/mnt/tokyoolympic/raw-data/EntriesGender.csv")
medals = spark.read.format("csv").option("header","true").load("/mnt/tokyoolympic/raw-data/Medals.csv")
teams = spark.read.format("csv").option("header","true").load("/mnt/tokyoolympic/raw-data/Teams.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #Transformation

# COMMAND ----------

athletes.show(), coaches.show(), gender.show(), medals.show(), teams.show()


# COMMAND ----------

print('Schema Athletes: ') 
athletes.printSchema()
print('Schema Coaches: ')
coaches.printSchema()
print('Schema EntriesGender: ')
gender.printSchema()
print('Schema Medals: ')
medals.printSchema()
print('Schema Teams: ')
teams.printSchema()


# COMMAND ----------

gender = gender.withColumn("Female", col("Female").cast(IntegerType()))\
    .withColumn("Male", col("Male").cast(IntegerType()))\
        .withColumn("Total", col("Total").cast(IntegerType()))


medals = medals.withColumn("Rank", col("Rank").cast(IntegerType()))\
.withColumn("Gold", col("Gold").cast(IntegerType()))\
    .withColumn("Silver", col("Silver").cast(IntegerType()))\
        .withColumn("Bronze", col("Bronze").cast(IntegerType()))\
            .withColumn("Total", col("Total").cast(IntegerType()))\
                .withColumn("Rank by Total", col("Rank by Total").cast(IntegerType()))

# COMMAND ----------

# MAGIC %md
# MAGIC #Exporting Data

# COMMAND ----------

athletes = athletes.write.mode('overwrite').option("header", 'true').csv("/mnt/tokyoolympic/transformed-data/Athletes")
coaches = coaches.write.mode('overwrite').option("header", 'true').csv("/mnt/tokyoolympic/transformed-data/Coaches")
gender = gender.write.mode('overwrite').option("header", 'true').csv("/mnt/tokyoolympic/transformed-data/Entries_Gender")
medals = medals.write.mode('overwrite').option("header", 'true').csv("/mnt/tokyoolympic/transformed-data/Medals")
teams = teams.write.mode('overwrite').option("header", 'true').csv("/mnt/tokyoolympic/transformed-data/Teams")
