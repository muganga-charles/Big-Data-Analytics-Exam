import os
import sys
import json
import shutil
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

warnings.filterwarnings("ignore")

from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, StringType, TimestampType,
)
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    VectorAssembler, StringIndexer, OneHotEncoder, StandardScaler,
)
from pyspark.ml.regression import (
    LinearRegression, RandomForestRegressor, GBTRegressor,
)
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# Reproducibility
SEED = 42
np.random.seed(SEED)

# Project paths (all relative so the repo is portable).
PROJECT_ROOT = Path(".").resolve() if Path("data").exists() else Path("..").resolve()
RAW_DIR     = PROJECT_ROOT / "data" / "raw"
PROC_DIR    = PROJECT_ROOT / "data" / "processed"
STREAM_IN   = PROJECT_ROOT / "data" / "stream_input"
STREAM_CKPT = PROJECT_ROOT / "data" / "stream_checkpoint"
STREAM_OUT  = PROJECT_ROOT / "data" / "stream_output"
MODEL_DIR   = PROJECT_ROOT / "models"
FIG_DIR     = PROJECT_ROOT / "outputs" / "figures"
METRICS_DIR = PROJECT_ROOT / "outputs" / "metrics"

for d in (PROC_DIR, STREAM_IN, STREAM_CKPT, STREAM_OUT, MODEL_DIR, FIG_DIR, METRICS_DIR):
    d.mkdir(parents=True, exist_ok=True)

print("Project root:", PROJECT_ROOT)

# %%
spark = (
    SparkSession.builder
    .appName("UCU_BDA_NYC_Taxi_Demand")
    .master("local[*]")                                    # use every core on the laptop
    .config("spark.driver.memory", "3g")                   # tuned for ~8 GB laptops
    .config("spark.sql.shuffle.partitions", "16")          # default 200 is wasteful locally
    .config("spark.sql.adaptive.enabled", "true")          # AQE: shrink shuffle stages
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("Spark", spark.version, "| cores:", spark.sparkContext.defaultParallelism)

# %% [markdown]
# ## 1 · Ingestion with an explicit schema
#
# Two design choices worth calling out:
#
# 1. **Explicit schema**, not `inferSchema`. Inference reads the data twice —
#    fine for a notebook demo, a waste on a laptop with 10 M+ rows.
# 2. **One function** that works with either CSV (raw drop) or Parquet
#    (our columnar store). The startup lands data as CSV and we promote it to
#    Parquet once; all downstream work reads Parquet.

# %%
TAXI_SCHEMA = StructType([
    StructField("VendorID",              IntegerType(), True),
    StructField("tpep_pickup_datetime",  TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count",       DoubleType(), True),
    StructField("trip_distance",         DoubleType(), True),
    StructField("RatecodeID",            DoubleType(), True),
    StructField("store_and_fwd_flag",    StringType(), True),
    StructField("PULocationID",          IntegerType(), True),
    StructField("DOLocationID",          IntegerType(), True),
    StructField("payment_type",          IntegerType(), True),
    StructField("fare_amount",           DoubleType(), True),
    StructField("extra",                 DoubleType(), True),
    StructField("mta_tax",               DoubleType(), True),
    StructField("tip_amount",            DoubleType(), True),
    StructField("tolls_amount",          DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount",          DoubleType(), True),
    StructField("congestion_surcharge",  DoubleType(), True),
])


def load_taxi(path: str | Path):
    """Load NYC taxi data from CSV or Parquet using an explicit schema."""
    p = str(path)
    if p.endswith(".parquet") or Path(p).is_dir():
        return spark.read.parquet(p)
    return (
        spark.read
        .option("header", True)
        .schema(TAXI_SCHEMA)
        .csv(p)
    )


# Find any CSV/parquet the user has dropped in data/raw/.
raw_candidates = sorted(list(RAW_DIR.glob("*.csv")) + list(RAW_DIR.glob("*.parquet")))
assert raw_candidates, (
    "No data found in data/raw/. Drop the NYC Taxi CSV/Parquet file(s) there "
    "or run: python src/generate_sample_data.py"
)
RAW_PATH = str(raw_candidates[0])
print("Loading:", RAW_PATH)

df_raw = load_taxi(RAW_PATH)
print("Raw rows:", df_raw.count(), "| columns:", len(df_raw.columns))
df_raw.printSchema()

# %% [markdown]
# ## 2 · Data quality & EDA
#
# A quick inventory of quality issues — these drive the cleaning decisions in
# the next cell. For a startup with no dedicated data-engineering team this
# audit step is where most of the risk lives.

# %%
total = df_raw.count()
quality = df_raw.agg(
    F.sum(F.col("passenger_count").isNull().cast("int")).alias("null_passenger"),
    F.sum(F.col("trip_distance").isNull().cast("int")).alias("null_distance"),
    F.sum(F.col("fare_amount").isNull().cast("int")).alias("null_fare"),
    F.sum((F.col("fare_amount") <= 0).cast("int")).alias("nonpositive_fare"),
    F.sum((F.col("trip_distance") <= 0).cast("int")).alias("nonpositive_distance"),
    F.sum((F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime")).cast("int"))
     .alias("bad_timestamps"),
).toPandas().T.rename(columns={0: "count"})
quality["pct"] = (quality["count"] / total * 100).round(3)
print("Quality audit (total rows = {:,})".format(total))
print(quality)

# %% [markdown]
# ## 3 · Cleaning & promotion to Parquet (offline-first batch layer)
#
# **Filters (each justified):**
# - Drop rows with non-positive fare or distance — refunds / data-entry errors.
# - Drop rows where drop-off precedes pickup — corrupt timestamps.
# - Clip trip distance at the 99.5th percentile to tame outlier long-haul trips.
# - Fill `passenger_count` nulls with the mode (1) — safe, most trips are solo.
# - Drop exact duplicates.
#
# **Why Parquet**: columnar + compressed + schema-embedded; 5-10× smaller than
# CSV and enables predicate push-down. Critical when RAM is the bottleneck.

# %%
# Percentile-based distance cap, computed on a sample for speed.
dist_cap = (
    df_raw.sample(fraction=0.1, seed=SEED)
    .approxQuantile("trip_distance", [0.995], 0.01)[0]
)
print(f"Capping trip_distance at {dist_cap:.2f} miles (99.5th percentile)")

df_clean = (
    df_raw
    .dropDuplicates()
    .filter(F.col("fare_amount") > 0)
    .filter(F.col("trip_distance") > 0)
    .filter(F.col("trip_distance") <= dist_cap)
    .filter(F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
    .withColumn(
        "passenger_count",
        F.when(F.col("passenger_count").isNull(), F.lit(1.0))
         .otherwise(F.col("passenger_count")),
    )
    .withColumn(
        "trip_duration_min",
        (F.col("tpep_dropoff_datetime").cast("long")
         - F.col("tpep_pickup_datetime").cast("long")) / 60.0,
    )
    .filter((F.col("trip_duration_min") >= 1) & (F.col("trip_duration_min") <= 180))
)

clean_count = df_clean.count()
print(f"After cleaning: {clean_count:,} rows "
      f"({(clean_count/total)*100:.2f}% of raw kept).")

# Write Parquet partitioned by pickup date — cheap time-range scans later.
df_clean = df_clean.withColumn(
    "pickup_date", F.to_date("tpep_pickup_datetime")
)
clean_path = str(PROC_DIR / "trips_clean.parquet")
(df_clean.write
    .mode("overwrite")
    .partitionBy("pickup_date")
    .parquet(clean_path))
print("Clean Parquet written:", clean_path)

# Reload from Parquet so downstream uses the compact form.
df = spark.read.parquet(clean_path)
df.cache()
print("Cached clean dataframe rows:", df.count())

# %% [markdown]
# ## 4 · Target construction — hourly zone demand
#
# We pivot from *row-per-trip* to *row-per-(zone, hour)* with a `pickup_count`
# target. This transforms a transactional stream into the supervised-learning
# panel that answers the stakeholder's question.

# %%
df_hour = (
    df.withColumn("pickup_hour", F.date_trunc("hour", "tpep_pickup_datetime"))
      .groupBy("PULocationID", "pickup_hour")
      .agg(
          F.count("*").alias("pickup_count"),
          F.avg("trip_distance").alias("avg_distance"),
          F.avg("fare_amount").alias("avg_fare"),
          F.avg("trip_duration_min").alias("avg_duration"),
      )
)
print("Hourly-zone panel rows:", df_hour.count())
df_hour.show(5, truncate=False)

# %% [markdown]
# ## 5 · Feature engineering
#
# **Temporal features** (from `pickup_hour`): hour-of-day, day-of-week,
# weekend flag, month. Cyclical encoding is skipped because tree models
# handle raw integers well and we are comparing a linear baseline anyway.
#
# **Lag features** (the real signal for time series): demand 1 hour ago,
# 24 hours ago (same hour yesterday), 168 hours ago (same hour last week),
# and a 24-hour rolling mean. These are the variables any competent forecast
# must beat a persistence baseline on.

# %%
df_feat = (
    df_hour
    .withColumn("hour",      F.hour("pickup_hour"))
    .withColumn("dow",       F.dayofweek("pickup_hour"))          # 1=Sun
    .withColumn("is_weekend",(F.col("dow").isin(1, 7)).cast("int"))
    .withColumn("month",     F.month("pickup_hour"))
    .withColumn("day",       F.dayofmonth("pickup_hour"))
)

# Lags require an ordered window *within* each zone.
w_zone = Window.partitionBy("PULocationID").orderBy("pickup_hour")
df_feat = (
    df_feat
    .withColumn("lag_1h",   F.lag("pickup_count", 1).over(w_zone))
    .withColumn("lag_24h",  F.lag("pickup_count", 24).over(w_zone))
    .withColumn("lag_168h", F.lag("pickup_count", 168).over(w_zone))
    .withColumn(
        "roll_mean_24h",
        F.avg("pickup_count").over(w_zone.rowsBetween(-24, -1)),
    )
)

# Drop the warm-up rows where lags are null (the first week of each zone).
df_feat = df_feat.dropna(subset=["lag_1h", "lag_24h", "lag_168h", "roll_mean_24h"])
print("Feature-complete rows:", df_feat.count())
df_feat.show(3, truncate=False)

# %% [markdown]
# ## 6 · Temporal train / validation / test split
#
# A random split would leak future hours into the past. We use a
# **strictly chronological split**: 70% train, 15% validation, 15% test — the
# industry-standard protocol for any time-series forecasting study.

# %%
time_bounds = df_feat.agg(
    F.min("pickup_hour").alias("lo"), F.max("pickup_hour").alias("hi")
).collect()[0]
t_lo, t_hi = time_bounds["lo"], time_bounds["hi"]
span = (t_hi - t_lo).total_seconds()
t_train_end = t_lo + pd.Timedelta(seconds=span * 0.70)
t_val_end   = t_lo + pd.Timedelta(seconds=span * 0.85)

print(f"Full window: {t_lo}  →  {t_hi}")
print(f"Train   end: {t_train_end}")
print(f"Valid   end: {t_val_end}")

train_df = df_feat.filter(F.col("pickup_hour") <  F.lit(t_train_end))
val_df   = df_feat.filter((F.col("pickup_hour") >= F.lit(t_train_end)) &
                          (F.col("pickup_hour") <  F.lit(t_val_end)))
test_df  = df_feat.filter(F.col("pickup_hour") >= F.lit(t_val_end))
print(f"Rows — train: {train_df.count():,}  val: {val_df.count():,}  test: {test_df.count():,}")

# %% [markdown]
# ## 7 · ML pipeline (Spark MLlib Pipeline API)
#
# A proper `Pipeline` rather than ad-hoc transforms because it (a) guarantees
# the same transforms at inference time — see §10 on train/serve skew, and
# (b) serialises cleanly to disk for deployment.
#
# **Stages**
# 1. `StringIndexer` on `PULocationID` (treat zone id as categorical).
# 2. `OneHotEncoder` on the indexed zone — linear models need it; tree models
#    tolerate it and we want a single pipeline across all three.
# 3. `VectorAssembler` combining all features into `features`.
# 4. `StandardScaler` — only strictly needed for LR but harmless for trees.
# 5. The estimator.
#
# We then train three models and pick on **validation RMSE**.

# %%
CAT_COLS = ["PULocationID"]
NUM_COLS = [
    "hour", "dow", "is_weekend", "month", "day",
    "avg_distance", "avg_fare", "avg_duration",
    "lag_1h", "lag_24h", "lag_168h", "roll_mean_24h",
]
TARGET = "pickup_count"

indexer = StringIndexer(
    inputCols=CAT_COLS, outputCols=[c + "_idx" for c in CAT_COLS],
    handleInvalid="keep",                        # unseen zones → extra bucket
)
encoder = OneHotEncoder(
    inputCols=[c + "_idx" for c in CAT_COLS],
    outputCols=[c + "_ohe" for c in CAT_COLS],
    handleInvalid="keep",
)
assembler = VectorAssembler(
    inputCols=[c + "_ohe" for c in CAT_COLS] + NUM_COLS,
    outputCol="features_raw",
    handleInvalid="skip",
)
scaler = StandardScaler(inputCol="features_raw", outputCol="features",
                        withMean=False, withStd=True)

PRE_STAGES = [indexer, encoder, assembler, scaler]


def make_pipeline(estimator):
    return Pipeline(stages=PRE_STAGES + [estimator])


rmse_eval = RegressionEvaluator(labelCol=TARGET, predictionCol="prediction",
                                metricName="rmse")
mae_eval  = RegressionEvaluator(labelCol=TARGET, predictionCol="prediction",
                                metricName="mae")
r2_eval   = RegressionEvaluator(labelCol=TARGET, predictionCol="prediction",
                                metricName="r2")


def evaluate(model, df_eval, label):
    pred = model.transform(df_eval)
    return {
        "model": label,
        "rmse": rmse_eval.evaluate(pred),
        "mae":  mae_eval.evaluate(pred),
        "r2":   r2_eval.evaluate(pred),
    }

# %% [markdown]
# ### 7.1 Baseline — persistence (no model)
#
# Before any ML, what does *"demand next hour = demand this hour"* score?
# Any model we ship must beat this.

# %%
persist_pred = val_df.withColumn("prediction", F.col("lag_1h").cast("double"))
baseline_metrics = {
    "model": "persistence_lag1h",
    "rmse": rmse_eval.evaluate(persist_pred),
    "mae":  mae_eval.evaluate(persist_pred),
    "r2":   r2_eval.evaluate(persist_pred),
}
print("Baseline (val):", baseline_metrics)

# %% [markdown]
# ### 7.2 Linear Regression, Random Forest, Gradient-Boosted Trees
#
# Moderate depth/trees — deeper forests blow up memory on an 8-GB laptop.

# %%
lr   = LinearRegression(featuresCol="features", labelCol=TARGET,
                        maxIter=50, regParam=0.1, elasticNetParam=0.0)
rf   = RandomForestRegressor(featuresCol="features", labelCol=TARGET,
                             numTrees=60, maxDepth=10, seed=SEED)
gbt  = GBTRegressor(featuresCol="features", labelCol=TARGET,
                    maxIter=60, maxDepth=6, stepSize=0.1, seed=SEED)

models = {}
val_scores = []
for name, est in [("linear_regression", lr),
                  ("random_forest", rf),
                  ("gbt", gbt)]:
    print(f"\nTraining {name} …")
    pipe = make_pipeline(est)
    m = pipe.fit(train_df)
    models[name] = m
    val_scores.append(evaluate(m, val_df, name))

val_scores.append(baseline_metrics)
val_df_pd = pd.DataFrame(val_scores).sort_values("rmse").reset_index(drop=True)
print("\nValidation leaderboard:")
print(val_df_pd.round(3).to_string(index=False))
val_df_pd.to_csv(METRICS_DIR / "val_leaderboard.csv", index=False)

# %% [markdown]
# ### 7.3 Test-set evaluation of the winner

# %%
winner_name = val_df_pd.iloc[0]["model"]
if winner_name == "persistence_lag1h":           # defensive
    winner_name = val_df_pd.iloc[1]["model"]
winner = models[winner_name]
print(f"Winner: {winner_name}")

test_scores = [evaluate(winner, test_df, f"{winner_name}_TEST")]
test_scores.append({
    "model": "persistence_lag1h_TEST",
    "rmse": rmse_eval.evaluate(test_df.withColumn("prediction", F.col("lag_1h").cast("double"))),
    "mae":  mae_eval.evaluate(test_df.withColumn("prediction", F.col("lag_1h").cast("double"))),
    "r2":   r2_eval.evaluate(test_df.withColumn("prediction", F.col("lag_1h").cast("double"))),
})
test_df_pd = pd.DataFrame(test_scores)
print(test_df_pd.round(3).to_string(index=False))
test_df_pd.to_csv(METRICS_DIR / "test_scores.csv", index=False)

# %% [markdown]
# ### 7.4 Persist the trained pipeline
#
# Saving the whole `PipelineModel` (not just the estimator) is what makes the
# offline → online hand-off safe: the serving code loads exactly the same
# transformers that saw training data.

# %%
model_path = str(MODEL_DIR / f"pipeline_{winner_name}")
if Path(model_path).exists():
    shutil.rmtree(model_path)
winner.write().overwrite().save(model_path)
print("Saved pipeline model to:", model_path)

# %% [markdown]
# ## 8 · Batch inference demo
#
# Load the saved pipeline and re-score the most recent day — this is what a
# nightly cron job on the office laptop would do.

# %%
from pyspark.ml import PipelineModel

loaded = PipelineModel.load(model_path)

last_day_start = t_hi - pd.Timedelta(days=1)
recent = df_feat.filter(F.col("pickup_hour") >= F.lit(last_day_start))
preds = (
    loaded.transform(recent)
    .select("PULocationID", "pickup_hour", TARGET,
            F.round("prediction", 2).alias("predicted_pickups"))
)
preds.orderBy("pickup_hour", "PULocationID").show(10, truncate=False)
preds.coalesce(1).write.mode("overwrite").parquet(
    str(PROC_DIR / "latest_batch_predictions.parquet")
)

# %% [markdown]
# ## 9 · Streaming simulation (Spark Structured Streaming)
#
# **Why simulated streaming?** Kafka/Kinesis is off the table — no cloud
# budget, intermittent internet. The realistic analogue on-prem is the
# **file source**: the dispatch system writes a trip-log CSV chunk to a
# watched folder; Spark picks it up, aggregates hourly pickups, and appends
# to a Parquet sink that the Ops dashboard reads.
#
# This gives us *micro-batch* latency (seconds) without any extra
# infrastructure — and the same PySpark code would scale to Kafka by changing
# one `readStream.format(...)` line.

# %%
# Split a slice of clean data into three "arrival" chunks.
for f in STREAM_IN.glob("*.csv"):
    f.unlink()
for f in STREAM_CKPT.glob("**/*"):
    if f.is_file():
        f.unlink()
if STREAM_OUT.exists():
    shutil.rmtree(STREAM_OUT)
STREAM_OUT.mkdir(parents=True, exist_ok=True)

stream_sample = (
    df.orderBy("tpep_pickup_datetime")
      .limit(30_000)
      .select("tpep_pickup_datetime", "PULocationID", "fare_amount", "trip_distance")
      .toPandas()
)
chunk_size = len(stream_sample) // 3 + 1
chunks = [stream_sample.iloc[i : i + chunk_size] for i in range(0, len(stream_sample), chunk_size)]
for i, ch in enumerate(chunks):
    ch.to_csv(STREAM_IN / f"arrival_{i:02d}.csv", index=False)
print("Seeded stream input:", [p.name for p in STREAM_IN.glob('*.csv')])

# %%
stream_schema = StructType([
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("PULocationID",         IntegerType(),  True),
    StructField("fare_amount",          DoubleType(),   True),
    StructField("trip_distance",        DoubleType(),   True),
])

stream_in = (
    spark.readStream
    .option("header", True)
    .schema(stream_schema)
    .csv(str(STREAM_IN))
)

agg_stream = (
    stream_in
    .withWatermark("tpep_pickup_datetime", "2 hours")
    .groupBy(
        F.window("tpep_pickup_datetime", "1 hour"),
        F.col("PULocationID"),
    )
    .agg(F.count("*").alias("pickup_count"),
         F.avg("fare_amount").alias("avg_fare"))
)

query = (
    agg_stream.writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", str(STREAM_OUT))
    .option("checkpointLocation", str(STREAM_CKPT))
    .trigger(processingTime="3 seconds")
    .start()
)

import time
time.sleep(20)                               # let three micro-batches fire
query.processAllAvailable()
query.stop()
print("Streaming query stopped. Output Parquet fragments:",
      len(list(STREAM_OUT.rglob('*.parquet'))))

stream_result = spark.read.parquet(str(STREAM_OUT))
print("Streaming aggregate rows:", stream_result.count())
stream_result.orderBy("window").show(5, truncate=False)

# %% [markdown]
# ## 10 · Insights & visualisations for the Operations Manager
#
# Four plots, each answering a decision the Ops Manager actually makes.

# %%
# Move to pandas for plotting — only aggregated data, so it fits in memory.
hourly = (
    df_feat.groupBy("hour").agg(F.avg("pickup_count").alias("avg_pickups"))
    .orderBy("hour").toPandas()
)
dow = (
    df_feat.groupBy("dow").agg(F.avg("pickup_count").alias("avg_pickups"))
    .orderBy("dow").toPandas()
)
top_zones = (
    df_feat.groupBy("PULocationID")
    .agg(F.sum("pickup_count").alias("total_pickups"))
    .orderBy(F.desc("total_pickups")).limit(15).toPandas()
)

# Figure 1 — demand by hour of day
fig, ax = plt.subplots(figsize=(9, 4))
ax.bar(hourly["hour"], hourly["avg_pickups"], color="#2E6F95")
ax.set_title("Average pickups per zone by hour of day")
ax.set_xlabel("Hour of day"); ax.set_ylabel("Avg pickups / zone / hour")
ax.set_xticks(range(0, 24))
plt.tight_layout()
plt.savefig(FIG_DIR / "01_demand_by_hour.png", dpi=150); plt.close()

# Figure 2 — demand by day of week
dow_labels = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
fig, ax = plt.subplots(figsize=(7, 4))
ax.bar([dow_labels[int(d) - 1] for d in dow["dow"]], dow["avg_pickups"],
       color="#E1812C")
ax.set_title("Average pickups per zone by day of week")
ax.set_ylabel("Avg pickups / zone / hour")
plt.tight_layout()
plt.savefig(FIG_DIR / "02_demand_by_dow.png", dpi=150); plt.close()

# Figure 3 — top 15 pickup zones
fig, ax = plt.subplots(figsize=(9, 4.5))
ax.bar(top_zones["PULocationID"].astype(str), top_zones["total_pickups"],
       color="#3A923A")
ax.set_title("Top 15 pickup zones by total demand (train+val+test window)")
ax.set_xlabel("Pickup Zone ID"); ax.set_ylabel("Total pickups")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(FIG_DIR / "03_top_zones.png", dpi=150); plt.close()

# Figure 4 — predicted vs actual on test set
pred_pdf = (
    winner.transform(test_df)
    .select(TARGET, "prediction").sample(fraction=0.2, seed=SEED).toPandas()
)
fig, ax = plt.subplots(figsize=(6, 6))
ax.scatter(pred_pdf[TARGET], pred_pdf["prediction"], alpha=0.3, s=10)
lims = [0, max(pred_pdf[TARGET].max(), pred_pdf["prediction"].max())]
ax.plot(lims, lims, "r--", linewidth=1)
ax.set_xlim(lims); ax.set_ylim(lims)
ax.set_xlabel("Actual pickups"); ax.set_ylabel("Predicted pickups")
ax.set_title(f"{winner_name} — predicted vs actual (test set)")
plt.tight_layout()
plt.savefig(FIG_DIR / "04_pred_vs_actual.png", dpi=150); plt.close()

print("Saved figures to", FIG_DIR)

# %% [markdown]
# ### Feature importance (tree-based models only)

# %%
try:
    last_stage = winner.stages[-1]
    if hasattr(last_stage, "featureImportances"):
        # Reconstruct feature names — OHE expands the zone column
        ohe_size = winner.stages[1].categorySizes[0]         # zone OHE width
        feat_names = [f"zone_ohe_{i}" for i in range(ohe_size)] + NUM_COLS
        imps = last_stage.featureImportances.toArray()
        imp_df = (pd.DataFrame({"feature": feat_names[:len(imps)], "imp": imps})
                    .sort_values("imp", ascending=False).head(15))

        fig, ax = plt.subplots(figsize=(8, 5))
        ax.barh(imp_df["feature"][::-1], imp_df["imp"][::-1], color="#7A4FB8")
        ax.set_title(f"Top-15 feature importances — {winner_name}")
        plt.tight_layout()
        plt.savefig(FIG_DIR / "05_feature_importance.png", dpi=150); plt.close()
        imp_df.to_csv(METRICS_DIR / "feature_importance.csv", index=False)
        print(imp_df.to_string(index=False))
except Exception as e:
    print("Feature importance skipped:", e)

# %% [markdown]
# ## 11 · Save the final metrics bundle

# %%
summary = {
    "winner": winner_name,
    "validation": val_df_pd.to_dict(orient="records"),
    "test": test_df_pd.to_dict(orient="records"),
    "train_window":  [str(t_lo), str(t_train_end)],
    "val_window":    [str(t_train_end), str(t_val_end)],
    "test_window":   [str(t_val_end), str(t_hi)],
    "rows": {
        "raw": total,
        "clean": clean_count,
        "hourly_panel": df_feat.count(),
    },
}
with open(METRICS_DIR / "run_summary.json", "w") as f:
    json.dump(summary, f, indent=2, default=str)
print(json.dumps(summary, indent=2, default=str))

# %%
spark.stop()
print("Pipeline complete. All artefacts written under `outputs/` and `models/`.")
