package com.example.crimestats

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

case class CrimeStatsDataAgg(District: String, name: String, crimes_total: Long, crimes_monthly: Long, Lat: Double, Lng: Double, count: Long)


object CrimeStatsAggr extends App {

  val spark = SparkSession.builder().master("local[*]").appName("CrimeStats").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val datafilepath = args(0)
  val dicfilepath = args(1)
  val outfilepath = args(2)

  val Crimes = spark
    .read
    .option("header", "true")
    .csv(datafilepath)
    .withColumnRenamed("Lat", "Latitude")
    .withColumn("Latitude", when($"Latitude".isNull, 0).otherwise($"Latitude"))
    .withColumn("Long", when($"Long".isNull, 0).otherwise($"Long"))
    .withColumn("Latitude", 'Latitude.cast(DoubleType))
    .withColumn("Long", 'Long.cast(DoubleType))

  val Offense_codes = spark
    .read
    .option("header", "true")
    .csv(dicfilepath)

  val crimes_total = Crimes
    .groupBy("district")
    .agg(count($"incident_number").alias("crimes_total"))
    .withColumnRenamed("district", "district_total")

  val crimes_avg_lat = Crimes
    .groupBy("district")
    .agg(avg($"Latitude").alias("lat"))
    .withColumnRenamed("district", "district_lat")

  val crimes_avg_long = Crimes
    .groupBy("district")
    .agg(avg($"Long").alias("lng"))
    .withColumnRenamed("district", "district_long")

  val crimes_by_monthly = Crimes
    .groupBy("district", "year","month")
    .agg(count($"incident_number").alias("crimes_in_month"))
    .groupBy("district")
    .agg(callUDF("percentile_approx", $"crimes_in_month", lit(0.5)).as("crimes_monthly"))
    .withColumnRenamed("district", "district_monthly")


  Crimes
    .join(broadcast(Offense_codes), Crimes("OFFENSE_CODE") === Offense_codes("CODE"))
    .join(broadcast(crimes_total), Crimes("DISTRICT") === crimes_total("district_total"))
    .join(broadcast(crimes_avg_lat), Crimes("DISTRICT") === crimes_avg_lat("district_lat"))
    .join(broadcast(crimes_avg_long), Crimes("DISTRICT") === crimes_avg_long("district_long"))
    .join(broadcast(crimes_by_monthly), Crimes("DISTRICT") === crimes_by_monthly("district_monthly"))
    .groupBy("DISTRICT", "name", "crimes_total", "crimes_monthly", "lat", "lng")
    .count()
    .orderBy('count.desc)
    .as[CrimeStatsDataAgg]
    .groupByKey(x => x.District)
    .flatMapGroups {
      case (districtKey, elements) => elements.toList.sortBy(x => -x.count).take(3)
    }
    .as[CrimeStatsDataAgg]
    .groupBy("DISTRICT", "CRIMES_TOTAL", "CRIMES_MONTHLY", "LAT", "LNG")
    .agg(collect_list(substring_index($"name", "-", 1)).alias("FREQUENT_CRIME_TYPES"))
    .write.parquet(outfilepath)


  spark.stop()
}