package com.example.crimestats

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql._

case class CrimeStatsDataAgg(District: String, name: String, crimes_total: Long, crimes_monthly: Long, Lat: Double, Lng: Double, count: Long)


object CrimeStatsAggr extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("CrimeStats")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val datafilepath = args(0)
  val dicfilepath = args(1)
  val outfilepath = args(2)

  val Crimes = spark
    .read
    .option("header", "true")
    .csv(datafilepath)
    .withColumn("district", when($"district".isNull, "N/A").otherwise($"district"))
    .withColumnRenamed("Lat", "Latitude")
    .withColumn("Latitude", when($"Latitude".isNull, 0).otherwise($"Latitude"))
    .withColumn("Long", when($"Long".isNull, 0).otherwise($"Long"))
    .withColumn("Latitude", 'Latitude.cast(DoubleType))
    .withColumn("Long", 'Long.cast(DoubleType))
    .withColumn("offense_code", 'offense_code.cast(DoubleType))

  val Offense_codes = spark
    .read
    .option("header", "true")
    .csv(dicfilepath)
    .withColumn("code", 'code.cast(DoubleType))
    .withColumn("name",substring_index($"name", "-", 1))

  val window_district = Window.partitionBy("district")

  val crimes_total = Crimes
    .withColumn("crimes_total", count("incident_number") over(window_district))
    .withColumn("lat", avg("Latitude") over(window_district))
    .withColumn("lng", avg("Long") over(window_district))
    .groupBy("district", "crimes_total", "lat", "lng")
    .count()

  val crimes_by_monthly = Crimes
    .groupBy("district", "year","month")
    .agg(count($"incident_number").alias("crimes_in_month"))
    .groupBy("district")
    .agg(callUDF("percentile_approx", $"crimes_in_month", lit(0.5)).as("crimes_monthly"))
    .withColumnRenamed("district", "district_monthly")

  val crimes_by_list = Crimes
    .join(broadcast(Offense_codes), Crimes("offense_code") === Offense_codes("code"))
    .groupBy("district", "name")
    .agg(count($"name").alias("offense_qty"))
    .withColumn("rn", row_number().over(window_district.orderBy(desc("offense_qty"))))
    .where("rn <= 3")
    .groupBy("district")
    .agg(collect_list("name").alias("frequent_crime_types"))
    .withColumnRenamed("district", "district_name_by_list")


  crimes_total
    .join(crimes_by_monthly, crimes_total("DISTRICT") === crimes_by_monthly("district_monthly"),"left_outer")
    .join(crimes_by_list, crimes_total("DISTRICT") === crimes_by_list("district_name_by_list"),"left_outer")
    .select("district", "crimes_total", "crimes_monthly", "frequent_crime_types", "lat", "lng")
    .orderBy("district")
    .repartition(1)
    .write.format("parquet").mode(SaveMode.Overwrite).save(outfilepath)

  //spark.read.parquet(outfilepath).show(false)

  spark.stop()
}