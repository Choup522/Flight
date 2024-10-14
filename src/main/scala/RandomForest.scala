import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.functions.unix_timestamp

case object RandomForest {

  // Conversion of columns to numerical values for string columns
  private def indexStringColumns(df: DataFrame, columns: Array[String]): DataFrame = {
    var tempDf = df
    for (col <- columns) {
      val indexer = new StringIndexer()
        .setInputCol(col)
        .setOutputCol(s"${col}_indexed")

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

  // Indexing labels
  private def labelIndexer(df: DataFrame, inputCol: String, outputCol: String): StringIndexer = {
    new StringIndexer()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setHandleInvalid("skip")
  }

  // Feature assembler
  private def featureAssembler(inputCols: Array[String], outputCol: String): VectorAssembler = {
    new VectorAssembler()
      .setInputCols(inputCols)
      .setOutputCol(outputCol)
  }

  // Random Forest model
  private def randomForestClassifier(labelCol: String, featuresCol: String): RandomForestClassifier = {
    new RandomForestClassifier()
      .setLabelCol(labelCol)
      .setFeaturesCol(featuresCol)
  }

  // Execution of the model
  def randomForest(df: DataFrame, labelCol: String, featureCols: Array[String]): Unit = {

    // Identify String and Timestamp/Date columns in the DataFrame
    val stringCols = df.dtypes.filter(_._2 == "StringType").map(_._1)
    // identify columns of type Timestamp or Date
    val timestampCols = df.dtypes.filter {
      case (_, dataType) => dataType == "TimestampType" || dataType == "DateType" }.map(_._1)

    // Process transformations on String and Timestamp columns
    var processedDF = indexStringColumns(df, stringCols)
    processedDF = convertDateColumns(processedDF, timestampCols)

    // Indexing steps
    val labelIndexerModel = labelIndexer(processedDF, labelCol, "indexedLabel")
    val assembler = featureAssembler(featureCols, "indexedFeatures")

    // Random Forest classifier
    val randomForest = randomForestClassifier("indexedLabel", "indexedFeatures")

    // Pipeline creation
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexerModel, assembler, randomForest))

    // Param grid for Grid Search
    val paramGrid = new ParamGridBuilder()
      .addGrid(randomForest.numTrees, Array(50)) // Testing different numbers of trees Array(50, 100, 150))
      .addGrid(randomForest.maxDepth, Array(5))    // Testing different depths Array(5, 10, 15))
      .build()

    // Cross-validator creation
    val crossValidator = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator()
        .setLabelCol("indexedLabel")
        .setPredictionCol("prediction")
        .setMetricName("accuracy"))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)  // Cross-validation with 5 folds

    // Splitting data into training and test sets
    val Array(trainingData, testData) = processedDF.randomSplit(Array(0.8, 0.2))

    // Fit the model using cross-validation
    val cvModel = crossValidator.fit(trainingData)

    // Make predictions on the test data
    val predictions = cvModel.transform(testData)

    // Evaluate the model
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)


    println(s"Cross-validated Test Accuracy = $accuracy") // A CHANGER POUR UNE SORTIE CSV

  }

}


