package bluebikes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MonthlyAndQuarterlyAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Monthly and Quarterly Analysis")
      .master("local[*]")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()

    val tripDataPath = "/Users/\\{EmailUserName\\}/Downloads/scala/2024-fall-csci-e88c/unpacked/"

    val tripDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(tripDataPath)

    val trendDF = tripDF
      .filter(col("starttime").isNotNull)
      .withColumn("year", year(to_timestamp(col("starttime"), "yyyy-MM-dd HH:mm:ss.SSSS")))
      .withColumn("month", month(to_timestamp(col("starttime"), "yyyy-MM-dd HH:mm:ss.SSSS")))
      .withColumn("quarter", quarter(to_timestamp(col("starttime"), "yyyy-MM-dd HH:mm:ss.SSSS")))
      .cache()

    val monthlyTrend = trendDF
      .groupBy("year", "month")
      .agg(count("*").alias("total_trips"), avg("tripduration").alias("average_duration"))
      .orderBy("year", "month")

    val quarterlyTrend = trendDF
      .groupBy("year", "quarter")
      .agg(count("*").alias("total_trips"), avg("tripduration").alias("average_duration"))
      .orderBy("year", "quarter")

    // Save monthly trend results to a CSV
    monthlyTrend.write
      .mode("overwrite") // Add this line to overwrite existing files
      .option("header", "true")
      .csv("./unpacked/monthly_trend.csv")

    // Save quarterly trend results to a CSV
    quarterlyTrend.write
      .mode("overwrite") // Add this line to overwrite existing files
      .option("header", "true")
      .csv("./unpacked/quarterly_trend.csv")


    spark.stop()
  }
}
