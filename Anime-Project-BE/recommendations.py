import os
os.environ["JAVA_HOME"] = "/Users/sribalac/Library/Java/JavaVirtualMachines/liberica-1.8.0_322"
os.environ["SPARK_HOME"] = "/Users/sribalac/Documents/Data Engg Tutorial/Assignment 1/spark-3.0.3-bin-hadoop2.7"

#Spark
import findspark
import random
findspark.init()
findspark.find()
import json
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

DATASET_ROOT = "/Users/sribalac/Documents/Data Engg Tutorial/Anime-Project-BE/resources/anime-dataset/"

spark = SparkSession.builder\
         .master("local[5]")\
         .appName("Colab")\
         .config('spark.ui.port', '4050')\
         .getOrCreate()

USER_RECOMMENDATIONS_PATH = "/Users/sribalac/Documents/Data Engg Tutorial/Anime-Project-BE/resources/als_model_user_recs"
ROOT = "/Users/sribalac/Documents/Data Engg Tutorial/Anime-Project-BE/resources/anime-dataset/"

anime_df = spark.read.format("csv").option("header", "true") \
    .option("headers", "true") \
    .option('escape','"') \
    .option("inferSchema", "true") \
    .load(ROOT + "anime_filtered.csv", sep=',')
user_df = spark.read.format("csv").option("header", "true") \
    .option("headers", "true") \
    .option('escape','"') \
    .option("inferSchema", "true") \
    .load(ROOT + "users_filtered.csv", sep=',')


def trending():
    trending_anime = spark.read.format('json').load("/Users/sribalac/Documents/Data Engg Tutorial/Anime-Project-BE/resources/output.json")

    trending_array = []

    trending_titles = list(map(lambda x: x[0: -2], list(trending_anime.toPandas()['anime_id'][0])))

    anime_length = len(trending_titles)

    for i in range(anime_length):
        trending_array.append({'anime_id': trending_anime.toPandas()['anime_id'][0][i], 'count':  trending_anime.toPandas()['count'][0][i]})

    trending_only_df = spark.createDataFrame(trending_array)

    trending_only_df = trending_only_df.withColumn('anime_id', F.regexp_replace("anime_id", ".0", ""))

    trending_anime_unordered = anime_df.select(["anime_id", "title", "image_url", "episodes", "related", "genre"]).filter(col("anime_id").isin(trending_titles))

    result = trending_anime_unordered.join(trending_only_df, trending_anime_unordered['anime_id'] == trending_only_df['anime_id']).orderBy(col("count").desc()).toJSON().map(lambda j: json.loads(j)).collect()

    # Exclude explicit content
    filtered_result = []
    for i in range(len(result)):
        if 'genre' in result[i] and ("Hentai" in result[i]['genre'] or "hentai" in result[i]['genre']):
            continue
        filtered_result.append(result[i])

    return filtered_result


def get_user_recommendations(user_name):
    user_recommended_anime_df = spark.read.parquet(USER_RECOMMENDATIONS_PATH)

    selected_user_df = user_df.select(["username", "user_id"]).where(col("username") == user_name)

    user_id = ""
    if selected_user_df.count() > 0:
        user_id = selected_user_df.first()[1]

    recommended_animes = user_recommended_anime_df.select(["recommendations"]).where(col("user_id") == user_id)
    recommended_animes_ratings_obj = []

    if recommended_animes.count() == 0 or selected_user_df.count() == 0:
        return anime_df.withColumn("userName", lit(user_name)).select(["anime_id", "title", "image_url", "episodes", "related", "genre"])\
            .where(col("episodes") > 0).orderBy(col('favorites').desc()).limit(10).toJSON().map(lambda j: json.loads(j)).collect()

    for anime in recommended_animes.first()[0]:
        recommended_animes_ratings_obj.append({'anime_id': anime[0], 'ratings': anime[1]})

    recommended_animes = list(map(lambda x: x['anime_id'], recommended_animes_ratings_obj))

    filtered_animes = anime_df.select(["anime_id", "title", "image_url", "episodes", "related", "genre"]).filter(anime_df["anime_id"].isin(recommended_animes))

    recommended_animes_ratings_df = spark.createDataFrame(recommended_animes_ratings_obj)

    filtered_animes.withColumn("username", lit(selected_user_df.first()[0]))

    result = filtered_animes.join(recommended_animes_ratings_df, filtered_animes['anime_id'] == recommended_animes_ratings_df['anime_id']).orderBy(col("ratings").desc()).toJSON().map(lambda j: json.loads(j)).collect()

    return result

