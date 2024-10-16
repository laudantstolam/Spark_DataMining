from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, count, mean, stddev, lit, regexp_replace, when, isnan

# Init
spark = SparkSession.builder.appName("HW0").getOrCreate()

# read the file in csv form
df = spark.read.csv("/home/ash/Downloads/data.txt", sep=';', header=True, inferSchema=True)

# replace ? with null
df = df.select([regexp_replace(col(c), "\\?", "null").alias(c) for c in df.columns])

#---------------------------------------------
target_columns = ['Global_active_power', 'Global_reactive_power', 'Voltage', 'Global_intensity']
#---------------------------------------------

# reform datatype
for index in target_columns:
 df = df.withColumn(index, col(index).cast("double"))

# drop null
df = df.na.drop()

#-------------------------------------------------------------------------
# (1) fint min, max, count

# min
min_df = df.select(
    [min(col(c)).alias(f"{c}") for c in target_columns]
).withColumn("Type", lit("Min"))

# max
max_df = df.select(
    [max(col(c)).alias(f"{c}") for c in target_columns]
).withColumn("Type", lit("Max"))

# count
count_df = df.select(
    [count(col(c)).alias(f"{c}") for c in target_columns]
).withColumn("Type", lit("Count"))

# combine
result_1_df = min_df.union(max_df).union(count_df)


# move description to the front
result_1_df = result_1_df.select("Type", *result_1_df.columns[:-1])

print("(1) min, max, count")
result_1_df.show(truncate=False)

#-------------------------------------------------------------------------
# (2) find mean, stddev

#mean
mean_df = df.select(
    [mean(col(c)).alias(f"{c}") for c in target_columns]
).withColumn("Type", lit("Mean"))

# stddev
stddev_df = df.select(
    [stddev(col(c)).alias(f"{c}") for c in target_columns]
).withColumn("Type", lit("Stddev"))

# combine
mean_stddev_df = mean_df.union(stddev_df)

# move description to the front
mean_stddev_df = mean_stddev_df.select("Type", *mean_stddev_df.columns[:-1])

print("(2) mean, stddev")
mean_stddev_df.show(truncate=False)

#-------------------------------------------------------------------------
# (3) Min-Max Normalization using dictionary

# Step 1: Find min and max values and store them in a dictionary
min_values = df.select([min(col(c)).alias(f"min_{c}") for c in target_columns]).first().asDict()
max_values = df.select([max(col(c)).alias(f"max_{c}") for c in target_columns]).first().asDict()

# Create dictionaries to store the min and max values
min_max_dict = {}
for c in target_columns:
    min_max_dict[c] = {
        'min': min_values[f"min_{c}"],
        'max': max_values[f"max_{c}"]
    }

# Step 2: Normalize each column using the dictionary
normalized_df = df
for c in target_columns:
    min_val = min_max_dict[c]['min']
    max_val = min_max_dict[c]['max']
    
    normalized_df = normalized_df.withColumn(
        f"normalized_{c}", 
        (col(c) - lit(min_val)) / (lit(max_val) - lit(min_val))
    )

print("(3) Min-Max Normalized Data")
normalized_df.select([f"normalized_{c}" for c in target_columns]).show(truncate=False)

# Step 3: Write the normalized data to a file
normalized_df.select([f"normalized_{c}" for c in target_columns]).write.csv("normalized_output.csv", header=True)
