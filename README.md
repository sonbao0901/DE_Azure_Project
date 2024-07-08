# Simple Data Engineer Pipeline Project on Azure
**In this project, I will show you how to build a simple data pipeline and do analysis on Azure.**

![system_diagram](https://github.com/sonbao0901/DE_Azure_Project/blob/main/images/Animation.gif)

**I built an ETL pipeline to ingest [Tokyo Olympic Data](https://github.com/sonbao0901/DE_Azure_Project/tree/main/data) from Github and loaded it into a ***Data Lake Gen 2***. After that, I use ***DataBricks*** to do some transformation for analytics purposes and then uploaded transformed data into ***Data Lake Gen 2***. Finally, I used ***Synapse Analytics*** to explore data.**

<!--truncate-->

## Step by Step

### Creating Subscription

To use Azure Serives, you need a *subcription* to give a way to logically group and manage Azure resources.

![image](https://github.com/sonbao0901/DE_Azure_Project/assets/104372010/603fe11d-13b7-4f42-920d-08df00f3d954)

After creating the resource, I would go into each application and started creating its own need resources.

### Storage Account

Creating a resource for Storage Account based on initial subscription and creating a resource groups for later uses with other services/applications (data factory, synapse, databricks). In the container, creating a container to store data. Inside created container, I created 2 folders for raw data and transformed data.

![image](https://github.com/sonbao0901/DE_Azure_Project/assets/104372010/55b705ef-a3ed-4712-ae76-16575c03f012)

![image](https://github.com/sonbao0901/DE_Azure_Project/assets/104372010/e5c6f885-c812-424d-bd8c-7467bbc5645b)

### Data Factory

Same as Storage Account, I also created a resource for Data Factory using the same resource groups so that data from each service/application can communicateto each other seamlessly and simplifies management tasks. After creating data factory resource, I went into its portal. Then, I created a pipeline to ingest data from Github csv file into Storage Account.

![image](https://github.com/sonbao0901/DE_Azure_Project/assets/104372010/1c8cf176-dc37-46f7-82d0-99307d68f6b7)

### DataBricks

Also using same a resource groups as 2 previous services/applications. To run code on DataBricks, I created a compute cluster to run Spark. The code below is an example code for data transformation using PySpark. After finishing transformations, I save all the files into Storage Account transform data folder.

![image](https://github.com/sonbao0901/DE_Azure_Project/assets/104372010/e88bd242-782f-44d8-a00c-af9b71b9044b)

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
"fs.azure.account.oauth2.client.id": "{client_Key}",
"fs.azure.account.oauth2.client.secret": '{secret_key}',    #can get secret key by creating an app using app registrations and then create a secret key to help databrick communication with data lake storage gen 2
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

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

![image](https://github.com/sonbao0901/DE_Azure_Project/assets/104372010/85182626-c5b9-4026-bc4d-5266871dcff9)

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

