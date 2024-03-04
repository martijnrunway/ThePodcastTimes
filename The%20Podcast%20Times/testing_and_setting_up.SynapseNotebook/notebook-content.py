# Synapse Analytics notebook source

# METADATA ********************

# META {
# META   "synapse": {
# META     "lakehouse": {
# META       "default_lakehouse": "4acdc359-29ec-461c-823d-4c2362fd8fda",
# META       "default_lakehouse_name": "podcastLakehouseRunway",
# META       "default_lakehouse_workspace_id": "68f7221e-bc96-4282-ba88-97edeb244d1d",
# META       "known_lakehouses": [
# META         {
# META           "id": "4acdc359-29ec-461c-823d-4c2362fd8fda"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************


# CELL ********************

podcast = "runway_12345"
column_name = "headline"
column_data = "World Domination!"

# CELL ********************

query = f"INSERT INTO podcastdata (podcast, {column_name}) VALUES ('{podcast}','{column_data}')"
print(query)
spark.sql(query)


# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS podcastData (
# MAGIC   podcast STRING,
# MAGIC   episodeName STRING
# MAGIC )
# MAGIC USING DELTA

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS articles (
# MAGIC   podcast STRING,
# MAGIC   episode STRING,
# MAGIC   headline STRING,
# MAGIC   article STRING,
# MAGIC   image STRING
# MAGIC )
# MAGIC USING DELTA

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS sentiment (
# MAGIC   podcast STRING,
# MAGIC   positive INTEGER,
# MAGIC   negative INTEGER,
# MAGIC   neutral INTEGER
# MAGIC )
# MAGIC USING DELTA

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE article

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * from articles

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO podcastdata
# MAGIC (podcast, headline)
# MAGIC VALUES ('runway_1234','World domination!')

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql.functions import col
from pyspark.sql.functions import split

# CELL ********************

podcast = "hommikujutt_2024-02-22"

# CELL ********************

query = f"SELECT * FROM transcripts WHERE podcast = '{podcast}'"
query

# CELL ********************

df = spark.sql("SELECT * FROM podcastLakehouse.transcripts LIMIT 1000")

# CELL ********************

df = spark.sql(f"SELECT * FROM podcastLakehouse.transcripts WHERE podcast = '{podcast}'")

# CELL ********************


# With Spark SQL, Please run the query onto the lakehouse which is from the same workspace as the current default lakehouse.

df = spark.sql("SELECT * FROM podcastLakehouse.transcripts LIMIT 1000")
display(df)

# CELL ********************

df = spark.sql("SELECT * FROM transcripts")
df = df.orderBy(col("podcast"), col("id"))



# CELL ********************

