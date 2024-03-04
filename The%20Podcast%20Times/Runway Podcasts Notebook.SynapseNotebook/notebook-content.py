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
# META     },
# META     "environment": {
# META       "environmentId": "55184254-9bb1-4765-afd0-6603352f8b2c",
# META       "workspaceId": "68f7221e-bc96-4282-ba88-97edeb244d1d"
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql.functions import col

# CELL ********************

podcast = "hommikujutt_2024-02-22"

# CELL ********************

podcast = "paevakaja_2024-02-21"

# CELL ********************

df = spark.sql(f"SELECT * FROM transcripts WHERE podcast = '{podcast}'")
df = df.orderBy (col("id"))

df.show(n=15)

# CELL ********************

collected_values = df.select("text_en").rdd.flatMap(lambda x: x).collect()

full_text = ' '.join(collected_values)
all_words = full_text.split(' ')
len(all_words)

# CELL ********************

sections = []
this_section = ""
for word in all_words:
    this_section += word + " "
    if len(this_section) > 3000 and word[len(word)-1] == ".":
        sections.append(this_section)
        print(len(this_section), this_section[0:50], "...")
        this_section = ""
sections.append(this_section)
print(len(this_section), this_section[0:50], "...")
len(sections)

# CELL ********************

sections[0]


# CELL ********************

import openai
from openai import AzureOpenAI

print(openai.__version__)

client = AzureOpenAI(
azure_endpoint="https://polite-ground-030dc3103.4.azurestaticapps.net/api/v1",
api_key="557c2b9b-3f8e-436b-ade0-c5b36d239b45",
api_version="2023-09-01-preview",
)


# CELL ********************

MESSAGES = [
{"role": "system", "content": """you are a funny bot"""},
{"role": "user", "content": "tell me a joke about pumpkins"},
{"role": "assistant","content": "Why do pumpkins sit on people's porches? They have no hands to knock on the door!"},
]
MESSAGES.append({"role": "user", "content": "tell me a joke about frogs"})

completion = client.chat.completions.create(model="gpt-35-turbo", messages=MESSAGES,temperature=0.9)
print(completion.choices[0].message.content)

# CELL ********************


# CELL ********************

#condense story in sections that together  fit into one new query
summaries = []
total_summary = ""

prompt = "summarize this in about " + str(int(3000/len(sections))) + " characters: "
print(prompt)
for section in sections:
    MESSAGES = [
        {"role": "system", "content": """you are a serious bot"""},
        {"role": "user", "content": "summarize this in max 100 characters: The president gave a speech and thousands of people were enthousiastic about the engaging content and way of bringing the message accross"},
        {"role": "assistant","content": "The presendent gave an engaging speech."},
    ]
    MESSAGES.append({"role": "user", "content": prompt + section})

    completion = client.chat.completions.create(model="gpt-35-turbo", messages=MESSAGES,temperature=0.9)
    summary = completion.choices[0].message.content
    print(summary)
    summaries.append(summary)
    total_summary += summary + " "
len(total_summary)

# CELL ********************

#now summarize the summaries
MESSAGES = []
MESSAGES.append({"role": "user", "content": "summarize this text in max 200 words in a newspaper article style: " + total_summary})

completion = client.chat.completions.create(model="gpt-35-turbo", messages=MESSAGES,temperature=0.9)
summary = completion.choices[0].message.content
print(summary)
len(summary)


# CELL ********************

escaped_summary = summary.replace("\'","\\'")
query1 = f"DELETE FROM podcastsummaries WHERE podcast = '{podcast}' AND type = 'full_summary'"
query2 = f"INSERT INTO podcastsummaries (podcast, type, text) VALUES ('{podcast}', 'full_summary', '{escaped_summary}')"
print(query1)
print(query2)

# CELL ********************

spark.sql(query1)
spark.sql(query2)

# CELL ********************

#now find the weather forecast
MESSAGES = []
MESSAGES.append({"role": "user", "content": "provide a the weather forecast for a newspaper article, from what is mentioned in this story: " + total_summary})

completion = client.chat.completions.create(model="gpt-35-turbo", messages=MESSAGES,temperature=0.9)
summary = completion.choices[0].message.content
print(summary)


# CELL ********************

#now find the weather forecast
MESSAGES = []
MESSAGES.append({"role": "user", "content": "provide a the weather forecast for a newspaper article, from what is mentioned in this story: " + sections[len(sections)-2]})

completion = client.chat.completions.create(model="gpt-35-turbo", messages=MESSAGES,temperature=0.9)
weather_forecast = completion.choices[0].message.content
print(weather_forecast)

# CELL ********************

#now find the weather forecast
MESSAGES = []
MESSAGES.append({"role": "user", "content": "provide a the weather forecast in a table with wind, temperatures and rows for locations mentioned, from what is mentioned in this weather_forecast: " + weather_forecast})

completion = client.chat.completions.create(model="gpt-35-turbo", messages=MESSAGES,temperature=0.9)
summary = completion.choices[0].message.content
print(summary)

# CELL ********************

summary

# CELL ********************

df = spark.sql("SELECT * FROM podcasts LIMIT 20")
display(df)

# MARKDOWN ********************


# CELL ********************


# With Spark SQL, Please run the query onto the lakehouse which is from the same workspace as the current default lakehouse.

df = spark.sql("SELECT * FROM podcastSummaries LIMIT 20")
display(df)

# CELL ********************

spark.sql("ALTER TABLE podcasts_old ADD COLUMN testColumn VARCHAR(1000)")

# CELL ********************

spark.sql("UPDATE podcasts_old SET testColumn = 'Martijn'")
