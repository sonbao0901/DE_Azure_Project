# Databricks notebook source
# MAGIC %md
# MAGIC #Configuration

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "",
"fs.azure.account.oauth2.client.secret": '',
"fs.azure.account.oauth2.client.endpoint": ""}

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
