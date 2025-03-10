package bluebikes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StationDataSparkAnalysis {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("Station Data Spark Analysis")
      .master("local[*]") // Adjust this for cluster mode if needed
      .getOrCreate()

    // Path to the CSV file
    val stationDataPath = "/Users/\\{EmailUserName\\}/Downloads/scala/2024-fall-csci-e88c/station_data/station_data.csv"
    println(s"Using path: $stationDataPath")


    // Read the station data CSV into a DataFrame
    val stationDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(stationDataPath)

    // Show the schema
    stationDF.printSchema()

    // Example Analysis: Total Capacity of All Stations
    val totalCapacity = stationDF.agg(sum("capacity").as("total_capacity"))
    totalCapacity.show()

    // Example Analysis: Count Stations by Capacity Bins
    val capacityBins = stationDF
      .withColumn("capacity_bin", when(col("capacity") < 10, "Small")
        .when(col("capacity") <= 20, "Medium")
        .otherwise("Large"))
      .groupBy("capacity_bin")
      .count()

    capacityBins.show()

    // Example Analysis: Station with the Highest Capacity
    val maxCapacityStation = stationDF.orderBy(col("capacity").desc).limit(1)
    maxCapacityStation.show()

    maxCapacityStation.write
        .option("header", "true")
        .csv("./station_data/max_capacity_station.csv")

    // Save Results
    capacityBins.write.option("header", "true").csv("./station_data/capacity_bins.csv")

    // Stop Spark Session
    spark.stop()
  }
}

