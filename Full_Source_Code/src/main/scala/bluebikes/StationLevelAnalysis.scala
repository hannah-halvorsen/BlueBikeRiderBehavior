package bluebikes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StationLevelAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Station-Level Analysis")
      .master("local[*]")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()

    val tripDataPath = "/Users/\\{EmailUserName\\}/Downloads/scala/2024-fall-csci-e88c/unpacked/20*-tripdata.csv"
    val stationDataPath = "/Users/\\{EmailUserName\\}/Downloads/scala/2024-fall-csci-e88c/station_data/station_data.csv"

    // Read trip data, rename columns, and add year/month
    val tripDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(tripDataPath)
      .withColumnRenamed("start station id", "start_station_id")
      .withColumnRenamed("start station name", "start_station_name")
      .withColumnRenamed("start station latitude", "start_station_latitude")
      .withColumnRenamed("start station longitude", "start_station_longitude")
      .withColumnRenamed("end station id", "end_station_id")
      .withColumnRenamed("end station name", "end_station_name")
      .withColumnRenamed("end station latitude", "end_station_latitude")
      .withColumnRenamed("end station longitude", "end_station_longitude")
      .withColumn("year", year(to_timestamp(col("starttime"), "yyyy-MM-dd HH:mm:ss")))
      .withColumn("month", month(to_timestamp(col("starttime"), "yyyy-MM-dd HH:mm:ss")))

    val stationDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(stationDataPath)

    // Normalize station names for joining
    val tripDFNormalized = tripDF
      .withColumn("join_station_name", trim(lower(col("start_station_name"))))

    val stationDFNormalized = stationDF
      .withColumn("join_station_name", trim(lower(col("name"))))

    // Join on station name
    val stationUsage = tripDFNormalized
      .join(stationDFNormalized, Seq("join_station_name"), "inner")
      .groupBy("station_id", "name", "year", "month")
      .agg(
        count("*").alias("trip_count"),
        avg("tripduration").alias("avg_duration")
      )
      .orderBy("station_id", "year", "month")

    // Write the results
    stationUsage.write
      .mode("overwrite")
      .option("header", "true")
      .csv("./unpacked/station_usage.csv")

    spark.stop()
  }
}

