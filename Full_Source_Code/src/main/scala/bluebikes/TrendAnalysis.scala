package bluebikes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TrendAnalysis {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session with legacy time parser policy
    val spark = SparkSession.builder()
      .appName("Bluebikes Trend Analysis")
      .master("local[*]")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY") // Set to LEGACY for older datetime parsing
      .getOrCreate()

     // Path to the trip data directory
    val tripDataPath = "/Users/\\{EmailUserName\\}/Downloads/scala/2024-fall-csci-e88c/unpacked/"



    // Read trip data into a DataFrame
    val tripDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(tripDataPath)

    // Extract year and month from the start time
    val trendDF = tripDF
      .filter(col("starttime").isNotNull) // Exclude rows with null start times
      .withColumn("year", year(to_timestamp(col("starttime"), "yyyy-MM-dd HH:mm:ss.SSSS")))
      .withColumn("month", month(to_timestamp(col("starttime"), "yyyy-MM-dd HH:mm:ss.SSSS")))
      .withColumn("quarter", quarter(to_timestamp(col("starttime"), "yyyy-MM-dd HH:mm:ss.SSSS")))
      .cache() // Cache for reuse in multiple computations


    // Monthly aggregation: Trip count and average duration
    val monthlyTrend = trendDF
      .groupBy("year", "month")
      .agg(
        count("*").alias("total_trips"),
        avg("tripduration").alias("average_duration")
      )
      .orderBy("year", "month")

    // Quarterly aggregation: Trip count and average duration
    val quarterlyTrend = trendDF
      .groupBy("year", "quarter")
      .agg(
        count("*").alias("total_trips"),
        avg("tripduration").alias("average_duration")
      )
      .orderBy("year", "quarter")

    // Save monthly trend results to a CSV
    monthlyTrend.write
      .option("header", "true")
      .csv("./unpacked/monthly_trend.csv")

    // Save quarterly trend results to a CSV
    quarterlyTrend.write
      .option("header", "true")
      .csv("./unpacked/quarterly_trend.csv")

    // Show a sample of the data
    monthlyTrend.show(10)
    quarterlyTrend.show(10)

    // Stop Spark Session
    spark.stop()
  }
}