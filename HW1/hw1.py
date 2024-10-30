from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, count, to_date, regexp_replace, explode, split, round, lit, substring, sum as _sum
import nltk
from nltk.corpus import stopwords

# DL stopwords
nltk.download('stopwords')
stop_words = set(stopwords.words('english'))

# INIT Spark
spark = SparkSession.builder.appName("HW1_gogo").config("spark.sql.legacy.timeParserPolicy", "LEGACY").getOrCreate()
df = spark.read.csv("spacenews.csv", sep=',', header=True, inferSchema=True)

# analyze null datas in each col
def print_analyze_result():
    columns_to_check = ["title", "url", "content", "author", "date"]
    null_counts = {col_name: df.filter(col(col_name).isNull()).count() for col_name in columns_to_check}
    for col_name, count in null_counts.items():
        print(f"col: '{col_name}' includes {count} NULL datas")
# print_analyze_result()

# clear data and remove punctuation
def filter_text(text_column):
    words = explode(split(lower(regexp_replace(text_column, r"[^a-zA-Z0-9\s]", "")), "\\s+"))
    return words

def calculate_frequency(df, column_name, by_date=False):
    df = df.filter(col("word") != "")  # remove spaces
    if by_date:
        daily_word_count = df.groupBy("date", "word").agg(count("word").alias("count")).orderBy(col("date"), col("count").desc())
        print(f"Per day for {column_name}")
        daily_word_count.show(truncate=False)
    else:
        total_word_count = df.groupBy("word").agg(count("word").alias("count")).orderBy(col("count").desc())
        print(f"In total for {column_name}")
        total_word_count.show(truncate=False)

# Formatting the date
df_total = df.withColumn("date", to_date(col("date"), "MMM dd, yyyy"))
df_filtered = df_total.filter(col("date").isNotNull())

# ----------------------<1+2>----------------------------------------

# Calculate total frequency with df_total
for col_name in ["title", "content"]:
    df_col_total = df_total.withColumn("word", filter_text(col(col_name)))
    calculate_frequency(df_col_total.filter(~col("word").isin(stop_words)), col_name)

# Use df_filtered for date-associated calculations
for col_name in ["title", "content"]:
    df_col_filtered = df_filtered.withColumn("word", filter_text(col(col_name)))
    calculate_frequency(df_col_filtered.filter(~col("word").isin(stop_words)), col_name, by_date=True)

# ----------------------<3>----------------------------------------
daily_article_count = df_filtered.groupBy("date").agg(count("*").cast("int").alias("daily_total"))
total_article_count = daily_article_count.agg(_sum(col("daily_total")).alias("all_total")).collect()[0]["all_total"]
daily_percentage = daily_article_count.withColumn("daily_percentage", round((col("daily_total") / lit(total_article_count)) * 100, 2))
print("Percentage of articles published per day")
daily_percentage.select("date", "daily_percentage").show(truncate=False)

# daily percentage of each author
author_daily_count = df_filtered.groupBy("date", "author").agg(count("*").cast("int").alias("author_daily_total"))
author_daily_percentage = author_daily_count.join(daily_article_count, "date").withColumn("% of articles", round((col("author_daily_total") / col("daily_total")) * 100, 2))
print("Percentage of articles published per author per day")
author_daily_percentage.select("date", "author", "% of articles").show(truncate=False)

# ----------------------<4>----------------------------------------
filtered_records = df_total.filter(
    (lower(col("title")).contains("space")) & 
    (lower(col("postexcerpt")).contains("space"))
)
filtered_records_truncated = filtered_records.select(
    substring("title", 1, 10).alias("title"),
    substring("url", 1, 10).alias("url"),
    substring("content", 1, 10).alias("content"),
    "author","date",
    substring("postexcerpt", 1, 10).alias("postexcerpt")
)

print("Records containing 'space' in both 'title' and 'postexcerpt'")
filtered_records_truncated.show(truncate=False)

# Save result to CSV file
output_path = "filtered_records.csv"
filtered_records_truncated.write.csv(output_path, header=True, mode="overwrite")