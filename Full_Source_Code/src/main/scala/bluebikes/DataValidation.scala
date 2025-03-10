package bluebikes

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object DataValidation {

  // Define the expected schema for trip data
  // Adjust field names as per your actual columns
  val expectedTripSchema: StructType = StructType(Seq(
    StructField("tripduration", IntegerType, nullable = true),
    StructField("starttime", StringType, nullable = true),
    StructField("stoptime", StringType, nullable = true),
    StructField("start_station_name", StringType, nullable = true),
    StructField("end_station_name", StringType, nullable = true),
    StructField("year", IntegerType, nullable = true),
    StructField("month", IntegerType, nullable = true)
    // Add any other columns you expect
  ))

  // Define the expected schema for station data
  val expectedStationSchema: StructType = StructType(Seq(
    StructField("station_id", StringType, nullable = false),
    StructField("name", StringType, nullable = false),
    StructField("lat", DoubleType, nullable = false),
    StructField("lon", DoubleType, nullable = false),
    StructField("capacity", IntegerType, nullable = false)
  ))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Data Validation and Schema Enforcement")
      .master("local[*]")
      .getOrCreate()

    val tripDataPath = "/Users/\\{EmailUserName\\}/Downloads/Scala/2024-fall-csci-e88c/unpacked/2022*-tripdata.csv" 
    val stationDataPath = "/Users/\\{EmailUserName\\}/Downloads/Scala/2024-fall-csci-e88c/station_data/station_data.csv"

    val tripDF = loadAndValidateTripData(spark, tripDataPath)
    val stationDF = loadAndValidateStationData(spark, stationDataPath)

    // Basic data quality checks
    validateNonNullColumns(tripDF, Seq("starttime", "end_station_name"))
    validateNonNullColumns(stationDF, Seq("station_id", "name"))

    // Example numeric validation: check for negative capacities
    val invalidCapacityCount = stationDF.filter(col("capacity") < 0).count()
    require(invalidCapacityCount == 0, "Found stations with negative capacity.")

    println("Data validation passed successfully!")

    spark.stop()
  }

  def loadAndValidateTripData(spark: SparkSession, path: String): DataFrame = {
    val df = spark.read
      .option("header", "true")
      .schema(expectedTripSchema) // Enforce schema here
      .csv(path)

    // If rows do not conform to schema, they'll be null or cause errors.

    df
  }

  def loadAndValidateStationData(spark: SparkSession, path: String): DataFrame = {
    val df = spark.read
      .option("header", "true")
      .schema(expectedStationSchema) // Enforce schema here
      .csv(path)
    df
  }

  def validateNonNullColumns(df: DataFrame, cols: Seq[String]): Unit = {
    cols.foreach { c =>
      val nullCount = df.filter(col(c).isNull).count()
      require(nullCount == 0, s"Column $c contains $nullCount null values.")
    }
  }
}
