import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.functions.{col, unix_timestamp}
import LoggerFactory.logger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

case object RandomForest {

  def indexStringColumns2(df: DataFrame, columns: Array[String]): DataFrame = {

    val futures = columns.map { colName =>
      Future {
        val indexer = new StringIndexer()
          .setInputCol(colName)
          .setOutputCol(s"${colName}_indexed")
          .setHandleInvalid("skip")

        indexer.fit(df).transform(df).drop(colName).withColumnRenamed(s"${colName}_indexed", colName)
      }
    }

    val indexedDfs = Await.result(Future.sequence(futures.toSeq), 10.minutes)
    indexedDfs.reduce(_ union _)
  }

  // Conversion of columns to numerical values for string columns
  private def indexStringColumns(df: DataFrame, columns: Array[String]): DataFrame = {
    var tempDf = df
    for (col <- columns) {
      val indexer = new StringIndexer()
        .setInputCol(col)
        .setOutputCol(s"${col}_indexed")
        .setHandleInvalid("skip")

      tempDf = indexer
        .fit(tempDf)
        .transform(tempDf).drop(col)
        .withColumnRenamed(s"${col}_indexed", col)
    }
    tempDf
  }

  // Conversion of columns to numerical values for date and timestamp columns
  private def convertDateColumns(df: DataFrame, columns: Array[String]): DataFrame = {
    var tempDf = df
    for (col <- columns) {
      tempDf = tempDf.
        withColumn(col, unix_timestamp(df(col)).cast("double"))
    }
    tempDf
  }

  // Execution of the model
  def randomForest(df: DataFrame, labelCol: String, featureCols: Array[String], numberOfTrees: Int, depth: Int, folds: Int): Map[String, Double]  = {

    try {

      logger.info("Verifying the values of inputs")

      // Validate inputs
      require(df != null && !df.isEmpty, "Input DataFrame is null or empty.")
      require(labelCol.nonEmpty, "Label column name cannot be empty.")
      require(featureCols.nonEmpty, "Feature columns cannot be empty.")
      require(numberOfTrees > 0, "Number of trees must be a positive integer.")
      require(depth > 0, "Maximum depth must be a positive integer.")
      require(folds > 1, "Number of folds for cross-validation must be greater than 1.")

      // Validate label column values
      val uniqueLabels = df.select(labelCol).distinct().collect().map(_.get(0))
      require(
        uniqueLabels.forall(label => label.isInstanceOf[Number] && label.asInstanceOf[Number].doubleValue >= 0),
        s"All labels in column $labelCol must be non-negative numbers. Found: ${uniqueLabels.mkString(", ")}"
      )

      // Identify String and Timestamp/Date columns in the DataFrame
      val stringCols = df.dtypes.filter(_._2 == "StringType").map(_._1)

      // identify columns of type Timestamp or Date
      val timestampCols = df.dtypes.filter { case (_, dataType) => dataType == "TimestampType" || dataType == "DateType" }.map(_._1)

      // Process transformations on String and Timestamp columns
      logger.info("Process transformation")
      var processedDF = indexStringColumns2(df, stringCols)
      processedDF = processedDF.withColumn(labelCol, col(labelCol).cast("int"))
      processedDF = convertDateColumns(processedDF, timestampCols)

      // Ensure all classes are present in the dataset
      val allClasses = processedDF.select(labelCol).distinct().collect().map(_.getInt(0)).toSet
      require(
        allClasses.nonEmpty,
        s"Dataset must contain at least one class in the label column. Found: ${allClasses.mkString(", ")}"
      )

      logger.info("Preparing the pipeline")

      // Indexing steps
      val labelIndexerModel = new StringIndexer()
        .setInputCol(labelCol)
        .setOutputCol("indexedLabel")
        .fit(processedDF)

      val assembler = new VectorAssembler()
        .setInputCols(featureCols)
        .setOutputCol("indexedFeatures")

      // Random Forest classifier
      val randomForest = new RandomForestClassifier()
        .setLabelCol("indexedLabel")
        .setFeaturesCol("indexedFeatures")
        .setNumTrees(numberOfTrees)
        .setMaxDepth(depth)

      // Pipeline creation
      val pipeline = new Pipeline()
        .setStages(Array(labelIndexerModel, assembler, randomForest))

      logger.info("Creating training et test set")

      // Splitting data into training and test sets
      val Array(trainingData, testData) = processedDF.randomSplit(Array(0.8, 0.2))

      // Train the model
      logger.info("Training the model")
      val model = pipeline.fit(trainingData)

      // Make predictions on the test data
      logger.info("Prediction phase")
      val predictions = model.transform(testData)

      // Ensure no null values in prediction columns
      require(
        predictions.select("indexedLabel", "prediction").filter(row => row.isNullAt(0) || row.isNullAt(1)).isEmpty,
        "Null values found in indexedLabel or prediction columns."
      )

      // Evaluate the model
      logger.info("Evaluation of the model")
      val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("indexedLabel")
        .setPredictionCol("prediction")

      val accuracy = evaluator.setMetricName("accuracy").evaluate(predictions)
      val f1Score = evaluator.setMetricName("f1").evaluate(predictions)
      val recall = evaluator.setMetricName("weightedRecall").evaluate(predictions)
      val precision = evaluator.setMetricName("weightedPrecision").evaluate(predictions)

      // Collect the metrics
      logger.info("Collect the metrics")
      val rfModel = model.stages.last.asInstanceOf[RandomForestClassificationModel]
      val metrics = Map(
        "accuracy" -> accuracy,
        "f1Score" -> f1Score,
        "recall" -> recall,
        "precision" -> precision,
        "numTrees" -> rfModel.getNumTrees.toDouble,
        "maxDepth" -> rfModel.getMaxDepth.toDouble
      )
      metrics

    } catch {
      case e: Exception =>
        logger.info("error in the pipeline")
        println(s"An error occurred: ${e.getMessage}")
        Map("error" -> Double.NaN)
    }
  }

  // Function to save metrics
  def saveMetricsAsCSV(spark: SparkSession, metrics: Map[String, Double], outputPath: String): Unit = {

    import spark.implicits._

    // Conversion of the metrics to a DataFrame
    val metricsDF = metrics.toSeq.toDF("metric", "value")

    // Save in CSV format
    metricsDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$outputPath/metrics.csv")

  }
}


