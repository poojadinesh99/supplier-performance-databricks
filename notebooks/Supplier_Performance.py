# Databricks notebook source
# MAGIC %md
# MAGIC # Supplier Performance Pipeline
# MAGIC
# MAGIC Synthetic SAP MM data (EKKO, EKPO, EKET, LIKP, LIPS, LFA1, MARA) → supplier KPIs and rating.
# MAGIC
# MAGIC | Layer | Tables | Content |
# MAGIC |---|---|---|
# MAGIC | Bronze | `bronze_<sap_table>` | Raw CSV files from the `sap_raw` volume, all columns as strings, plus load metadata |
# MAGIC | Silver | `supplier_perf_silver` | One row per delivery item, matched to its schedule line, typed and validated, with line-level KPIs |
# MAGIC | Gold | `supplier_performance_gold` | One row per supplier: on-time rate, fill rate, average delay, rating |

# COMMAND ----------

from pyspark.sql import functions as F

SCHEMA = "workspace.default"
RAW_PATH = "/Volumes/workspace/default/sap_raw"

SOURCES = {
    "ekko": "EKKO_PO_HEADER.csv",
    "ekpo": "EKPO_PO_ITEM.csv",
    "eket": "EKET_SCHEDULE_LINES.csv",
    "likp": "LIKP_DELIVERY_HEADER.csv",
    "lips": "LIPS_DELIVERY_ITEM.csv",
    "lfa1": "LFA1_VENDOR_MASTER.csv",
    "mara": "MARA_MATERIAL_MASTER.csv",
}

# Tolerance when matching a delivered quantity to a schedule line's goods-receipt quantity
QTY_TOLERANCE = 0.005

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: load raw CSV files
# MAGIC Everything is read as strings so SAP keys keep their leading zeros; typing happens in silver.

# COMMAND ----------

for name, file_name in SOURCES.items():
    raw = (
        spark.read
        .option("header", True)
        .option("sep", ";")
        .option("inferSchema", False)
        .csv(f"{RAW_PATH}/{file_name}")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    if raw.limit(1).count() == 0:
        raise ValueError(f"Bronze load failed: {file_name} is empty or missing")
    (
        raw.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{SCHEMA}.bronze_{name}")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: type, join and validate
# MAGIC
# MAGIC **Delivery → schedule line matching.** LIPS references the PO item (`EBELN`/`EBELP`) but not the
# MAGIC schedule line (`ETENR`). 46 PO items have two schedule lines and 41 have two deliveries, so joining
# MAGIC both tables on `EBELN`/`EBELP` alone would pair every delivery with every schedule line of the item.
# MAGIC In this dataset each delivery's quantity (`LIPS.LFIMG`) equals the goods-receipt quantity
# MAGIC (`EKET.WEMNG`) of exactly one schedule line of the same item, so that is used as the matching key.
# MAGIC The check below stops the pipeline if any delivery item matches zero or several schedule lines.

# COMMAND ----------

def bronze(name):
    return spark.table(f"{SCHEMA}.bronze_{name}")


ekko = bronze("ekko").select(
    "EBELN",
    F.col("LIFNR").cast("bigint").alias("LIFNR"),
)

ekpo = bronze("ekpo").select(
    "EBELN",
    F.col("EBELP").cast("int").alias("EBELP"),
    "MATNR",
    "MATKL",
    F.coalesce(F.trim("LOEKZ"), F.lit("")).alias("deletion_flag"),
)

eket = bronze("eket").select(
    "EBELN",
    F.col("EBELP").cast("int").alias("EBELP"),
    F.col("ETENR").cast("int").alias("ETENR"),
    F.to_date("EINDT").alias("planned_date"),
    F.col("MENGE").cast("double").alias("scheduled_qty"),
    F.col("WEMNG").cast("double").alias("gr_qty"),
)

likp = bronze("likp").select(
    "VBELN",
    F.to_date("WADAT_IST").alias("actual_delivery_date"),
)

lips = bronze("lips").select(
    "VBELN",
    F.col("POSNR").cast("int").alias("POSNR"),
    "EBELN",
    F.col("EBELP").cast("int").alias("EBELP"),
    F.col("LFIMG").cast("double").alias("delivered_qty"),
)

lfa1 = bronze("lfa1").select(
    F.col("LIFNR").cast("bigint").alias("LIFNR"),
    "NAME1",
)

# COMMAND ----------

deliveries = lips.join(likp, "VBELN", "inner").alias("d")

matched = deliveries.join(
    eket.alias("s"),
    (F.col("d.EBELN") == F.col("s.EBELN"))
    & (F.col("d.EBELP") == F.col("s.EBELP"))
    & (F.abs(F.col("d.delivered_qty") - F.col("s.gr_qty")) < QTY_TOLERANCE),
    "left",
)

# Data quality: every delivery item must match exactly one schedule line
match_counts = matched.groupBy("d.VBELN", "d.POSNR").agg(F.count("s.ETENR").alias("n_matches"))
bad_matches = match_counts.filter(F.col("n_matches") != 1).count()
if bad_matches > 0:
    raise ValueError(f"{bad_matches} delivery items do not match exactly one schedule line")

# COMMAND ----------

joined = (
    matched.select(
        F.col("d.VBELN").alias("VBELN"),
        F.col("d.POSNR").alias("POSNR"),
        F.col("d.EBELN").alias("EBELN"),
        F.col("d.EBELP").alias("EBELP"),
        F.col("s.ETENR").alias("ETENR"),
        F.col("s.planned_date").alias("planned_date"),
        F.col("d.actual_delivery_date").alias("actual_delivery_date"),
        F.col("s.scheduled_qty").alias("scheduled_qty"),
        F.col("d.delivered_qty").alias("delivered_qty"),
    )
    .join(ekpo, ["EBELN", "EBELP"], "inner")
    .join(ekko, "EBELN", "inner")
)

# Data quality: the joins to PO item and PO header must not lose or duplicate delivery items
n_delivery_items = deliveries.count()
if joined.count() != n_delivery_items:
    raise ValueError("Joining PO item/header changed the number of delivery items")

silver = (
    joined
    .filter(F.col("scheduled_qty") > 0)
    .filter(F.col("delivered_qty") >= 0)
    .filter(F.col("planned_date").isNotNull())
    .filter(F.col("actual_delivery_date").isNotNull())
    .dropDuplicates(["VBELN", "POSNR"])
    .withColumn(
        "on_time_flag",
        F.when(F.col("actual_delivery_date") <= F.col("planned_date"), 1).otherwise(0),
    )
    .withColumn(
        "delay_days",
        F.greatest(F.datediff("actual_delivery_date", "planned_date"), F.lit(0)),
    )
    .withColumn(
        "fill_rate",
        F.least(F.col("delivered_qty") / F.col("scheduled_qty"), F.lit(1.0)),
    )
)

# Rows dropped by the validity filters (e.g. a delivery without an actual delivery date)
n_excluded = n_delivery_items - silver.count()
print(f"Delivery items: {n_delivery_items}, excluded by validity filters: {n_excluded}")

(
    silver.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SCHEMA}.supplier_perf_silver")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: supplier KPIs and rating
# MAGIC
# MAGIC | Bewertung | Termintreue |
# MAGIC |---|---|
# MAGIC | Sehr gut | ≥ 90 % |
# MAGIC | Gut | ≥ 75 % und < 90 % |
# MAGIC | Verbesserungsbedarf | < 75 % |

# COMMAND ----------

gold = (
    spark.table(f"{SCHEMA}.supplier_perf_silver")
    .groupBy("LIFNR")
    .agg(
        F.avg("on_time_flag").alias("on_time_rate"),
        F.avg("delay_days").alias("avg_delay_days"),
        F.avg("fill_rate").alias("fill_rate"),
        F.count("*").alias("delivery_items"),
    )
    .join(lfa1, "LIFNR", "left")
    .withColumn(
        "Lieferantenbewertung",
        F.when(F.col("on_time_rate") >= 0.9, "Sehr gut")
        .when(F.col("on_time_rate") >= 0.75, "Gut")
        .otherwise("Verbesserungsbedarf"),
    )
    .select(
        F.col("LIFNR").alias("Lieferant_ID"),
        F.col("NAME1").alias("Lieferant_Name"),
        F.col("on_time_rate").alias("Termintreue"),
        F.col("avg_delay_days").alias("Durchschnittliche_Verspätung_Tage"),
        F.col("fill_rate").alias("Lieferfüllgrad"),
        "Lieferantenbewertung",
        F.col("delivery_items").alias("Anzahl_Lieferpositionen"),
    )
)

(
    gold.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SCHEMA}.supplier_performance_gold")
)

display(spark.table(f"{SCHEMA}.supplier_performance_gold").orderBy(F.desc("Durchschnittliche_Verspätung_Tage")))
