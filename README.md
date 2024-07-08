# Simple Data Engineer Pipeline Project on Azure
**In this project, I will show you how to build a simple data pipeline and do analysis on Azure.**

![system_diagram](https://github.com/sonbao0901/DE_Azure_Project/blob/main/images/Animation.gif)

**I built an ETL pipeline to ingest [Tokyo Olympic Data](https://github.com/sonbao0901/DE_Azure_Project/tree/main/data) from Github and loaded it into a ***Data Lake Gen 2***. After that, I use ***DataBricks*** to do some transformation for analytics purposes and then uploaded transformed data into ***Data Lake Gen 2***. Finally, I used ***Synapse Analytics*** to explore data.**

<!--truncate-->

## Step by Step

### Creating Subscription

To use Azure Serives, you need a *subcription* to give a way to logically group and manage Azure resources.

![image](https://github.com/sonbao0901/DE_Azure_Project/assets/104372010/24b3adb7-5bc5-4525-a1de-52c77632047a)

After creating the resource, I would go into each application and started creating its own need resources.

### Storage Account

Creating a resource for Storage Account based on initial subscription and creating a resource groups for later uses with other services/applications (data factory, synapse, databricks). Also, creating 2 folders to store raw data and transformed data.

### Data Factory

Same as Storage Account, I also created a resource for Data Factory using the same resource groups so that data from each service/application can communicateto each other seamlessly and simplifies management tasks. Creating a pipeline to ingest data from Github into Storage Account.

### DataBricks

Also using same a resource groups as 2 previous services/applications. To run code on DataBricks, I created a compute cluster to run Spark. The code below is an example code for data transformation using PySpark. After finishing transformations, I save all the files into Storage Account transform data folder.

```python
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
```

### Synapse Analytics

Using the same resource group as other applications. In this service, I ingested transformed data from Data Lake Gen 2 (ADLS gen 2). Then, I started writing SQL script for analysis purposes. The SQL scripts below is a example for SQL analysis:

```sql
--Count the number of athletes from each country
SELECT 
        Country, 
        COUNT(*) as TotalAthletes
FROM 
        athletes
GROUP BY 
        Country
ORDER BY 
        TotalAthletes ASC;



--Calculate the total medals won by each country
SELECT 
        Team_Country,
        SUM(Gold) AS Gold_Win, 
        SUM(Silver) AS Silver_Win, 
        SUM(Bronze) AS Bronze_Win
FROM 
        Medals
GROUP BY 
        Team_Country
ORDER BY 
        2 DESC, 
        3 DESC, 
        4 DESC;

SELECT 
        *
FROM
        Entries_Gender;

```

