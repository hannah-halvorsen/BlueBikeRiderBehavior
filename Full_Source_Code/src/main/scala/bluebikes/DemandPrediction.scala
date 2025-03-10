package bluebikes

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.functions.col

object DemandPrediction {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Demand Prediction")
      .master("local[*]")
      .getOrCreate()

    // Load the monthly_trend.csv file
    val monthlyTrendPath = "/Users/\\{EmailUserName\\}/Downloads/Scala/2024-fall-csci-e88c/unpacked/monthly_trend.csv"
    val monthlyDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(monthlyTrendPath)

    // Filter out rows with null year and month (e.g., the first row)
    val cleanDF = monthlyDF.filter("year IS NOT NULL AND month IS NOT NULL")

    // Prepare the features for ML: Use `month` and `average_duration`
    val assembler = new VectorAssembler()
      .setInputCols(Array("month", "average_duration"))
      .setOutputCol("features")

    val mlDF = assembler.transform(cleanDF).select("features", "total_trips")

    // Train-test split
    val Array(trainData, testData) = mlDF.randomSplit(Array(0.8, 0.2))

    // Train a Linear Regression model
    val lr = new LinearRegression()
      .setLabelCol("total_trips")
      .setFeaturesCol("features")
    val lrModel = lr.fit(trainData)

    // Evaluate the model
    val predictions = lrModel.transform(testData)
    val evaluator = new RegressionEvaluator()
      .setLabelCol("total_trips")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val rmse = evaluator.evaluate(predictions)
    println(s"Root Mean Squared Error (RMSE): $rmse")

    // Display sample predictions
    predictions.select("features", "total_trips", "prediction").show(10)

    // Save predictions to a CSV file
    val outputPredictionsPath = "./unpacked/predictions.csv"
    val predictionsToSave = predictions.withColumn("features", col("features").cast("string"))
    predictionsToSave.select("features", "total_trips", "prediction")
      .write
      .mode("overwrite") // Overwrite if the file already exists
      .option("header", "true")
      .csv(outputPredictionsPath)

    println(s"Predictions saved to $outputPredictionsPath")

    // Stop the Spark session
    spark.stop()
  }
}
