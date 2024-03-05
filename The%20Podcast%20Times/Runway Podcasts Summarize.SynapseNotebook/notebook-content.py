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

#pip install bing-image-urls

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql.functions import udf, col, when
import openai
from openai import AzureOpenAI
from bing_image_urls import bing_image_urls
import json
from pyspark.sql.types import StringType

# CELL ********************

def split_text_in_sections(full_text):
    all_words = full_text.split(' ')
    print ("words: ", len(all_words))

    #split in sections
    sections = []
    this_section = ""
    for word in all_words:
        this_section += word + " "
        if len(this_section) > 3000 and word[len(word)-1] == ".":
            sections.append(this_section)
            print(len(this_section), this_section[0:50], "...")
            this_section = ""
    #remaining words in last section
    sections.append(this_section)
    print(len(this_section), this_section[0:50], "...")
    print("sections:",len(sections))
    return sections

def init_openAI():
    client = AzureOpenAI(
    azure_endpoint="https://polite-ground-030dc3103.4.azurestaticapps.net/api/v1",
    api_key="557c2b9b-3f8e-436b-ade0-c5b36d239b45",
    api_version="2023-09-01-preview",
    )
    return client

def summarize_section(openAI_client, section, size):
    prompt = "summarize this in about " + str(size) + " characters: "
    #print(prompt)
    MESSAGES = [
        {"role": "system", "content": """you are a serious bot"""},
        {"role": "user", "content": "summarize this in max 100 characters: The president gave a speech and thousands of people were enthousiastic about the engaging content and way of bringing the message accross"},
        {"role": "assistant","content": "The president gave an engaging speech."},
    ]
    MESSAGES.append({"role": "user", "content": prompt + section})

    try:
        completion = openAI_client.chat.completions.create(model="gpt-35-turbo", messages=MESSAGES,temperature=0.9)
        summary = completion.choices[0].message.content
        return summary
    except Exception as e:
        print("problem summarizing")
        print(e)
        print("SECTION TEXT:", section)
    #when it fails, just return nothing for this section
    return ""

def write_article(openAI_client, summary, prompt):
    #write an article based on prompt provided, e.g. "summarize this text:"
    MESSAGES = []
    MESSAGES.append({"role": "user", "content": prompt + summary})

    completion = openAI_client.chat.completions.create(model="gpt-35-turbo", messages=MESSAGES,temperature=0.9)
    article = completion.choices[0].message.content
    return article


def write_news_article(openAI_client, summary):
    #write a news article based on the summarized story
    article = write_article (openAI_client, summary, "summarize this text in max 200 words in a newspaper article style: ")
    return article


def write_result_to_table(podcast, summary_type, summary):
    escaped_summary = summary.replace("\'","\\'")
    query1 = f"DELETE FROM podcastsummaries WHERE podcast = '{podcast}' AND type = '{summary_type}'"
    query2 = f"INSERT INTO podcastsummaries (podcast, type, text) VALUES ('{podcast}', '{summary_type}', '{escaped_summary}')"
    #print(query1)
    #print(query2)
    spark.sql(query1)
    spark.sql(query2)

def write_article_to_table(podcast, headline, article, image, program_info):
    #episode is displayable name of podcast episode
    episode = program_info["name"] + " " + podcast.split("_", 1)[-1]
    escaped_headline = headline.replace("\'","\\'")
    escaped_article = article.replace("\'","\\'")
    escaped_image = image.replace("\'","\\'")
    query1 = f"DELETE FROM articles WHERE podcast = '{podcast}'"
    query2 = f"INSERT INTO articles (podcast, headline, article, image, episode) VALUES ('{podcast}', '{escaped_headline}', '{escaped_article}', '{escaped_image}', '{episode}')"
    #print(query1)
    #print(query2)
    spark.sql(query1)
    spark.sql(query2)

def write_sentiment(podcast, positive, negative, neutral):
    query1 = f"DELETE FROM sentiment WHERE podcast = '{podcast}'"
    query2 = f"INSERT INTO sentiment (podcast, positive, negative, neutral) VALUES ('{podcast}', '{positive}', '{negative}', '{neutral}')"
    #print(query1)
    #print(query2)
    spark.sql(query1)
    spark.sql(query2)


def condense_story(openAI_client, sections):
    #condense story in sections that together  fit into one new query
    summaries = []
    total_summary = ""
    section_size = int(3000/len(sections))

    for section in sections:
        summary = summarize_section(openAI_client, section, section_size)
        print(summary)
        summaries.append(summary)
        total_summary += summary + " "
    print("Total summary length:", len(total_summary))
    return total_summary

def get_transcript(podcast):
    df = spark.sql(f"SELECT * FROM transcripts WHERE podcast = '{podcast}'")
    df = df.orderBy (col("id"))
    collected_values = df.select("text_en").rdd.flatMap(lambda x: x).collect()
    full_text = ' '.join(collected_values)
    sections = split_text_in_sections(full_text)
    return sections

#read previously created summary to be able to skip some steps on existing data
def get_summary(podcast):
    df = spark.sql(f"SELECT text FROM podcastsummaries WHERE podcast = '{podcast}' AND type = 'full_summary'")
    text = df.select("text").collect()[0][0]
    return text

#read previously created article to be able to skip some steps on existing data
def get_article(podcast):
    df = spark.sql(f"SELECT article FROM articles WHERE podcast = '{podcast}'")
    text = df.select("article").collect()[0][0]
    return text

# CELL ********************

def find_image(openAI_client, article):
    succeed = False
    try_nr = 0
    imgUrl = ""
    while not succeed and try_nr < 4:
        try_nr +- 1
        searchImage = write_article (openAI_client, article, "write a search query to find a picture for this article: ")
        print("try ",try_nr,", search:", searchImage)
        imgUrls = bing_image_urls(searchImage, limit=3)
        if len(imgUrls) > 0:
            imgUrl = imgUrls[0]
            succeed = True
    return imgUrl

# CELL ********************

def get_program(podcast):
    podcast_data = spark.sql(f"SELECT * FROM podcasts, programs WHERE podcast = '{podcast}' AND podcasts.program = programs.program").collect()
    program = {
        "program": podcast_data[0]["program"],
        "name" : podcast_data[0]["name"]
        }
    return program

# CELL ********************

def update_newest_from_program(program):
    #find newest podcast
    podcast_data = spark.sql(f"SELECT * FROM podcasts WHERE program = '{program}' ORDER BY date DESC LIMIT 1").collect()
    if podcast_data:
        newest_podcast = podcast_data[0]["podcast"]
        print(newest_podcast, program)
        spark.sql(f"DELETE FROM newest WHERE program = '{program}'")
        spark.sql(f"INSERT INTO newest (podcast, program) VALUES ('{newest_podcast}', '{program}')")

def update_newest(podcast):
    #get program
    program = get_program(podcast)
    update_newest_from_program(program)

# CELL ********************

def get_sentiment(openAI_client, podcast, article):
    prompt =  "\nGive the overall sentiment analysis to text above with %. \
           \nNo need for any additional text, desired output in json: \
           \n{{'Positive': '..%', 'Negative': '..%'', 'Neutral': '..%'}}"

    sentiment_success = False
    raw_sentiment = write_article(openAI_client,article, prompt)
    try:
        new_dict = json.loads(str(raw_sentiment).lower().replace("'", "\""))
        positive = int(new_dict.get('positive').rstrip('%'))
        negative = int(new_dict.get('negative').rstrip('%'))
        neutral  = int(new_dict.get('neutral').rstrip('%'))
        sentiment_success = True
    except Exception as e:
        print(f"Error sentiment analysis, {e}")
    print("sentiment:" , sentiment_success, positive, negative, neutral)
    
    if sentiment_success:
        write_sentiment(podcast, positive, negative, neutral)

# CELL ********************

def get_article(podcast):
    articles_data = spark.sql(f"SELECT * FROM articles WHERE podcast = '{podcast}'")
    if articles_data.count() > 0:
        article_data = articles_data.collect()[0]
        return article_data["article"], article_data["headline"], article_data["image"], article_data["episode"]
    else:
        print ("article not found")
        return None, None, None, None


# CELL ********************

#podcast = "huvitaja_2024-02-29"
#article, headline, image, episode = get_article(podcast)

# CELL ********************


def summarize(podcast):
    openAI_client = init_openAI()

    print("summarizing:", podcast)
    sections = get_transcript(podcast)

    program_info = get_program(podcast)
    print ("program info:"  ,program_info)

    total_summary = condense_story(openAI_client, sections)
    #write_result_to_table(podcast, "full_summary", total_summary)
    
    print("Writing article for", podcast)
    article = write_news_article(openAI_client, total_summary)
    #print(article)
    print("article length:" , len(article))
    #write_result_to_table(podcast, "article", article)

    headline = write_article (openAI_client, article, "write a newspaper headline for this newspaper article : ")
    print("headline:", len(headline), headline)
    #write_result_to_table(podcast, "headline", headline)

    #find a matching image with the article
    imgUrl = find_image(openAI_client, article)
    print(imgUrl)
    #write_result_to_table(podcast, "image", imgUrl)

    #write the article to the article table
    write_article_to_table(podcast, headline, article, imgUrl, program_info)
    print(f"podcast {podcast} done")

    #extract sentiment from article
    get_sentiment(openAI_client, podcast, total_summary)
    #update newest podcast info
    #update_newest(podcast)

# CELL ********************

def list_podcasts(redo=False):
    df = spark.sql(f"SELECT * FROM podcasts ORDER BY date DESC")
    # when redo, just run again what we had
    if redo:
        return df
    else:
        #find all podcasts without articles
        #summaries in db
        sum_df = spark.sql("SELECT podcast, headline FROM articles WHERE headline IS NOT NULL ")
        #join them
        combined_df = df.join(sum_df, on="podcast", how="left").orderBy("date", ascending=False)
        #podcasts without headline are to be done 
        todo_df = combined_df.filter(col("headline").isNull())
        return todo_df

# CELL ********************

#sum_df = spark.sql("SELECT * FROM articles WHERE podcast = 'hommikujutt_2024-02-16'")

# CELL ********************

#list = list_podcasts(True).collect()
#for row in list:
#    podcast = row["podcast"]
#    article = get_article(podcast)
#    summary = get_summary(podcast)
#    #sentiment = get_sentiment(openAI_client, podcast, summary)


# CELL ********************

def list_recent_podcasts(redo):
    todo_df = list_podcasts(redo)
    recent_df = todo_df.filter(col("date") > "2024-02-16")
    return recent_df

def summarize_recent_podcasts(redo=False):
    #summarize all recent podcasts
    recent_df = list_recent_podcasts(redo)
    print(f'processing {recent_df.count()} podcasts.')
    for row in recent_df.collect():
        print (row.podcast)
        summarize(row.podcast)
    print("done summarizing recent podcasts")

# CELL ********************

#find and store a new image for a given podcast
def update_image(podcast):
    openAI_client = init_openAI()
    article = get_article(podcast)
    article
    imgUrl = find_image(openAI_client, article)
    print(imgUrl)
    #write_result_to_table(podcast, "image", imgUrl)

# CELL ********************

summarize_recent_podcasts()

# CELL ********************

#code used to initialize some tables - should not need it in future
def initialize():
    programs = spark.sql("SELECT * from PROGRAMS").collect()
    for programRow in programs:
        program = programRow["program"]
        print(program)
        update_newest_from_program(program)

# CELL ********************

#find any stories left without images and try those again
#empty_images = spark.sql("SELECT podcast FROM podcasts WHERE podcast IN (SELECT podcast FROM podcastsummaries WHERE type='article') AND podcast NOT IN (SELECT podcast FROM podcastsummaries WHERE type='image') ORDER by date DESC").collect()
#print(empty_images)
#for row in empty_images:
#    podcast = row["podcast"]
#    print (podcast)
#    update_image(podcast)

# CELL ********************

def remove_red_characters(column):
    try:
        if (value := int(column)) >= 60:
            return "Happy"
        return "Negative"
    except Exception as e:
        return "Neutral"


df = spark.sql("SELECT * FROM sentiment")
df_udf = udf(remove_red_characters, StringType())
df = df.withColumn('analysis_res', df_udf(col('positive')))
df.write.mode("overwrite").saveAsTable("sentiment_analysis")

# CELL ********************

