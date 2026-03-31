from pyspark.sql.functions import col, sum, count, desc
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IPL").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Load both files
matches = spark.read.csv("/Users/shainky/Downloads/archive/matches.csv", header=True, inferSchema=True)
deliveries = spark.read.csv("/Users/shainky/Downloads/archive/deliveries.csv", header=True, inferSchema=True)

# Quick look
print("=== MATCHES ===")
matches.printSchema()
matches.show(3)

print("=== DELIVERIES ===")
deliveries.printSchema()
deliveries.show(3)
# 1. Most IPL titles per team
print("=== MOST IPL TITLES ===")
matches.groupBy("winner") \
    .count() \
    .orderBy(desc("count")) \
    .show(10)

# 2. Top 10 run scorers all time
print("=== TOP 10 RUN SCORERS ===")
deliveries.groupBy("batsman") \
    .agg(sum("batsman_runs").alias("total_runs")) \
    .orderBy(desc("total_runs")) \
    .show(10)

# 3. Top 10 wicket takers
print("=== TOP 10 WICKET TAKERS ===")
deliveries.filter(col("player_dismissed").isNotNull()) \
    .groupBy("bowler") \
    .count() \
    .withColumnRenamed("count", "wickets") \
    .orderBy(desc("wickets")) \
    .show(10)

# Spark SQL - your SQL skills work directly!
matches.createOrReplaceTempView("matches")
deliveries.createOrReplaceTempView("deliveries")

print("=== TOSS WIN = MATCH WIN? ===")
spark.sql("""
    SELECT 
        toss_decision,
        COUNT(*) as total,
        SUM(CASE WHEN toss_winner = winner THEN 1 ELSE 0 END) as toss_also_won,
        ROUND(SUM(CASE WHEN toss_winner = winner THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as win_pct
    FROM matches
    GROUP BY toss_decision
""").show()

from pyspark.sql.functions import round as spark_round

print("=== PLAYER OF THE MATCH vs THEIR RUN TALLY ===")

# Total runs per batsman from deliveries
run_tally = deliveries.groupBy("batsman") \
    .agg(sum("batsman_runs").alias("total_runs"))

# Count how many times each player won player of the match
pom = matches.groupBy("player_of_match") \
    .count() \
    .withColumnRenamed("count", "pom_awards")

# Join them together
pom.join(run_tally, pom.player_of_match == run_tally.batsman, "left") \
    .select("player_of_match", "pom_awards", "total_runs") \
    .orderBy(desc("pom_awards")) \
    .show(10)

from pyspark.sql.functions import rank
from pyspark.sql.window import Window

# Runs per batsman per season
runs_per_season = deliveries.join(
    matches.select("id", "season"),
    deliveries.match_id == matches.id
).groupBy("season", "batsman") \
 .agg(sum("batsman_runs").alias("runs"))

# Rank batsmen within each season
window = Window.partitionBy("season").orderBy(desc("runs"))

runs_per_season.withColumn("rank", rank().over(window)) \
    .filter(col("rank") == 1) \
    .orderBy("season") \
    .show(20)