package bluebikes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GeospatialHourlyAnalysis {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("Bluebikes Geospatial Hourly Analysis")
      .master("local[*]") // Adjust for cluster mode if needed
      .getOrCreate()

    // Correct Path to the directory containing all CSV files
    val tripDataDir = "/Users/\\{EmailUserName\\}/Downloads/scala/2024-fall-csci-e88c/unpacked/"
    println(s"Using path: $tripDataDir")

    // Read only 2022 CSV files into a single DataFrame
    val tripDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(tripDataDir + "2022*.csv")

    // Show schema to verify column names
    tripDF.printSchema()

    // Geospatial and Temporal Analysis
    val geospatialTrends = tripDF
      .withColumn("hour", hour(to_timestamp(col("starttime"), "yyyy-MM-dd HH:mm:ss")))
      .groupBy("start station name", "hour")
      .agg(count("*").as("total_trips"))
      .orderBy(col("total_trips").desc)

    // Show the top 10 busiest stations and hours
    geospatialTrends.show(10)

    // Save results to a directory
    val outputDir = "./unpacked/geospatial_hourly_trends_2022/"
    geospatialTrends.write
      .option("header", "true")
      .mode("overwrite") // Overwrite existing output
      .csv(outputDir)

    // Stop Spark Session
    spark.stop()
  }
}

