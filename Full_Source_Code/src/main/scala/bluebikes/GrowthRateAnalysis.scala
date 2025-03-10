package bluebikes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object GrowthRateAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Growth Rate Analysis")
      .master("local[*]")
      .getOrCreate()

    val monthlyTrendPath = "/Users/\\{EmailUserName\\}/Downloads/Scala/2024-fall-csci-e88c/unpacked/monthly_trend.csv"
    val monthlyTrend = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(monthlyTrendPath)

    val monthlyWindow = Window.orderBy("year", "month")
    val growthRates = monthlyTrend
      .withColumn("prev_total_trips", lag("total_trips", 1).over(monthlyWindow))
      .withColumn("growth_rate",
        when(col("prev_total_trips").isNotNull, 
          (col("total_trips") - col("prev_total_trips")) / col("prev_total_trips")
        ).otherwise(lit(0))
      )

    growthRates.write.option("header", "true").csv("./unpacked/growth_rates.csv")

    spark.stop()
  }
}
