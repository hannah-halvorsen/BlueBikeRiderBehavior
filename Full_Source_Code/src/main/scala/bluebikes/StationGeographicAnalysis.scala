package bluebikes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StationGeographicAnalysis {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("Station Geographic Analysis")
      .master("local[*]") // Adjust for cluster mode if needed
      .getOrCreate()

    // Path to the CSV file
    val stationDataPath = "/Users/\\{EmailUserName\\}/Downloads/scala/2024-fall-csci-e88c/station_data/station_data.csv"

    // Read the station data CSV into a DataFrame
    val stationDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(stationDataPath)

    // Show schema to verify column names
    stationDF.printSchema()

    // Define geographic bins
    val geographicBins = stationDF.withColumn("lat_bin", when(col("lat") < 42.3, "South")
      .when(col("lat").between(42.3, 42.4), "Central")
      .otherwise("North"))
      .withColumn("lon_bin", when(col("lon") < -71.1, "West")
      .otherwise("East"))

    // Aggregate capacity by geographic region
    val geographicDistribution = geographicBins
      .groupBy("lat_bin", "lon_bin")
      .agg(sum("capacity").as("total_capacity"))
      .orderBy("lat_bin", "lon_bin")

    // Show the geographic distribution results
    geographicDistribution.show()

    // Save the results to a CSV file
    geographicDistribution.write
      .option("header", "true")
      .csv("./station_data/geographic_distribution.csv")

    // Stop Spark Session
    spark.stop()
  }
}
