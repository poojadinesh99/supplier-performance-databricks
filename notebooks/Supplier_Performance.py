# Databricks notebook source
from pyspark.sql.functions import col, regexp_replace, when, datediff, avg

# -----------------------------------
# BRONZE LAYER (RAW DATA INGESTION)
#-----------------------------------

ekko = spark.table("ekko_po_header")
ekpo = spark.table("ekpo_po_item")
eket = spark.table("eket_schedule_lines")
lips = spark.table("lips_delivery_item")
likp = spark.table("likp_delivery_header")
lfa1 = spark.table("lfa_1_vendor_master")

# -----------------------------------
# SELECT + STANDARDIZE SCHEMA
# -----------------------------------

ekpo_sel = ekpo.select(
    col("EBELN"),
    col("EBELP"),
    col("MENGE").alias("po_quantity")
)

eket_sel = eket.select(
    col("EBELN"),
    col("EBELP"),
    col("MENGE").alias("scheduled_quantity"),
    col("EINDT").alias("planned_date")
)

lips_sel = lips.select(
    col("VBELN"),
    col("EBELN"),
    col("EBELP"),
    col("LFIMG").alias("delivered_quantity")
)

likp_sel = likp.select(
    col("VBELN"),
    col("WADAT_IST").alias("actual_delivery_date")
)
# -----------------------------------
# SILVER LAYER (CLEANING + JOIN + TRANSFORMATION)
# -----------------------------------

# -----------------------------------
# JOIN (FACT TABLE)
# -----------------------------------

df = ekpo_sel \
    .join(ekko.select("EBELN", "LIFNR"), "EBELN", "left") \
    .join(eket_sel, ["EBELN", "EBELP"], "left") \
    .join(lips_sel, ["EBELN", "EBELP"], "left") \
    .join(likp_sel, "VBELN", "left")

# -----------------------------------
# DATA CLEANING
# -----------------------------------

df_clean = df \
    .withColumn(
        "scheduled_qty",
        regexp_replace(col("scheduled_quantity"), "[^0-9.-]", "").cast("double")
    ) \
    .withColumn(
        "delivered_qty",
        regexp_replace(col("delivered_quantity"), "[^0-9.-]", "").cast("double")
    )

# -----------------------------------
# FILTER VALID DATA
# -----------------------------------

df_clean = df_clean \
    .filter(col("scheduled_qty").isNotNull()) \
    .filter(col("delivered_qty").isNotNull()) \
    .filter(col("scheduled_qty") > 0) \
    .filter(col("delivered_qty") >= 0) \
    .filter(col("planned_date").isNotNull()) \
    .filter(col("actual_delivery_date").isNotNull())

# -----------------------------------
# REMOVE DUPLICATES
# -----------------------------------

df_clean = df_clean.dropDuplicates(["EBELN", "EBELP", "VBELN"])

# -----------------------------------
# KPI CALCULATION
# -----------------------------------

df_kpi = df_clean \
    .withColumn(
        "on_time_flag",
        when(col("actual_delivery_date") <= col("planned_date"), 1).otherwise(0)
    ) \
    .withColumn(
        "delay_days",
        datediff(col("actual_delivery_date"), col("planned_date"))
    ) \
    .withColumn(
        "delay_days_clipped",
        when(col("delay_days") < 0, 0).otherwise(col("delay_days"))
    ) \
    .withColumn(
        "fill_rate",
        when(col("delivered_qty") >= col("scheduled_qty"), 1.0)
        .otherwise(col("delivered_qty") / col("scheduled_qty"))
    )

# -----------------------------------
# AGGREGATE TO SUPPLIER LEVEL
# -----------------------------------

supplier_perf = df_kpi.groupBy("LIFNR").agg(
    avg("on_time_flag").alias("on_time_rate"),
    avg("delay_days_clipped").alias("avg_delay_days"),
    avg("fill_rate").alias("fill_rate")
)

# -----------------------------------
# JOIN SUPPLIER MASTER (DIMENSION)
# -----------------------------------

supplier_perf = supplier_perf.join(
    lfa1.select("LIFNR", "NAME1"),
    "LIFNR",
    "left"
)

# -----------------------------------
# BUSINESS CLASSIFICATION (GERMAN)
# -----------------------------------

supplier_perf = supplier_perf.withColumn(
    "Lieferantenbewertung",
    when(col("on_time_rate") >= 0.9, "Sehr gut")
    .when(col("on_time_rate") >= 0.75, "Gut")
    .otherwise("Verbesserungsbedarf")
)

# -----------------------------------
# FINAL OUTPUT (BUSINESS-FRIENDLY)
# -----------------------------------

supplier_perf_final = supplier_perf.select(
    col("LIFNR").alias("Lieferant_ID"),
    col("NAME1").alias("Lieferant_Name"),
    col("on_time_rate").alias("Termintreue"),
    col("avg_delay_days").alias("Durchschnittliche_Verspätung_Tage"),
    col("fill_rate").alias("Lieferfüllgrad"),
    col("Lieferantenbewertung")
)

# -----------------------------------
# GOLD LAYER (KPI + BUSINESS LOGIC)
# -----------------------------------
supplier_perf_final.write.mode("overwrite").saveAsTable("supplier_performance_gold")

# -----------------------------------
# DISPLAY
# -----------------------------------

display(supplier_perf_final)
