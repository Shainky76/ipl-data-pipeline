from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, desc, rank, round as spark_round, when
from pyspark.sql.window import Window

# ── 1. INIT ──────────────────────────────────────────────────────────────
spark = SparkSession.builder.appName("IPL_Pipeline").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print(">>> Pipeline started...")

# ── 2. INGEST ─────────────────────────────────────────────────────────────
matches = spark.read.csv("/Users/shainky/Downloads/archive/matches.csv", header=True, inferSchema=True)
deliveries = spark.read.csv("/Users/shainky/Downloads/archive/deliveries.csv", header=True, inferSchema=True)

print(f">>> Loaded {matches.count()} matches, {deliveries.count()} deliveries")

# ── 3. TRANSFORM ──────────────────────────────────────────────────────────

# T1: Most IPL titles
titles = matches.filter(col("winner").isNotNull()) \
    .groupBy("winner") \
    .count() \
    .withColumnRenamed("count", "titles") \
    .orderBy(desc("titles"))

# T2: Top run scorers
top_scorers = deliveries.groupBy("batsman") \
    .agg(sum("batsman_runs").alias("total_runs")) \
    .orderBy(desc("total_runs"))

# T3: Top wicket takers
top_wickets = deliveries.filter(col("player_dismissed").isNotNull()) \
    .groupBy("bowler") \
    .agg(count("player_dismissed").alias("wickets")) \
    .orderBy(desc("wickets"))

# T4: Orange cap per season
runs_per_season = deliveries.join(
    matches.select("id", "season"),
    deliveries.match_id == matches.id
).groupBy("season", "batsman") \
 .agg(sum("batsman_runs").alias("runs"))

window = Window.partitionBy("season").orderBy(desc("runs"))

orange_cap = runs_per_season \
    .withColumn("rank", rank().over(window)) \
    .filter(col("rank") == 1) \
    .drop("rank") \
    .orderBy("season")

# T5: Toss impact
toss_impact = matches.groupBy("toss_decision") \
    .agg(
        count("*").alias("total"),
        sum(when(col("toss_winner") == col("winner"), 1).otherwise(0)).alias("toss_also_won")
    ) \
    .withColumn("win_pct", spark_round(col("toss_also_won") * 100.0 / col("total"), 1)) \
    .orderBy("toss_decision")

print(">>> Transforms done")

# ── 4. LOAD (write output) ────────────────────────────────────────────────
output_path = "/Users/shainky/Data_Engineer/DE_Training/ipl_output"

titles.coalesce(1).write.mode("overwrite").csv(f"{output_path}/titles", header=True)
top_scorers.coalesce(1).write.mode("overwrite").csv(f"{output_path}/top_scorers", header=True)
top_wickets.coalesce(1).write.mode("overwrite").csv(f"{output_path}/top_wickets", header=True)
orange_cap.coalesce(1).write.mode("overwrite").csv(f"{output_path}/orange_cap", header=True)
toss_impact.coalesce(1).write.mode("overwrite").csv(f"{output_path}/toss_impact", header=True)

print(">>> Output written to:", output_path)
print(">>> Pipeline complete ✓")