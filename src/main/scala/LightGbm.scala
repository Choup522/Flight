import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}

//import com.microsoft.ml.spark.LightGBMClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

case object LightGbm {

  // Function to index the target column
  private def indexTargetColumn(data: DataFrame, targetColumn: String): DataFrame = {
    val indexer = new StringIndexer()
      .setInputCol(targetColumn)
      .setOutputCol("label")
    indexer.fit(data).transform(data)
  }

  // Function to assemble features into a `features` column
  private def assembleFeatures(data: DataFrame, targetColumn: String): DataFrame = {
    val featureColumns = data.columns.filterNot(col => col == targetColumn || col == "label")
    val assembler = new VectorAssembler()
      .setInputCols(featureColumns)
      .setOutputCol("features")
    assembler.transform(data)
  }

  // function to configure and return a LightGBM classifier
  //private def createLightGBMClassifier(learningRate: Int, numLeaves: Int, NumIterations: Int): LightGBMClassifier = {
  //  new LightGBMClassifier()
  //    .setLabelCol("label")
  //    .setFeaturesCol("features")
  //    .setObjective("binary")
  //    .setLearningRate(0.1)
  //    .setNumLeaves(31)
  //    .setNumIterations(100)
  //    .setMaxDepth(-1)
  //}

  // Function to train the model
  //private def trainModel(trainData: DataFrame, classifier: LightGBMClassifier) = {
  //  classifier.fit(trainData)
  //}

  // Function to predict
  //private def makePredictions(model: LightGBMClassifier, testData: DataFrame): DataFrame = {
  //  model.transform(testData)
  //}

  // Fonction pour évaluer le modèle avec ROC-AUC
  def evaluateModel(predictions: DataFrame): Double = {
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("prediction")
      .setMetricName("areaUnderROC")
    evaluator.evaluate(predictions)
  }

  // Utilisation des fonctions
  //val dataIndexed = indexTargetColumn(data, "target_column")
  //val dataWithFeatures = assembleFeatures(dataIndexed, "target_column")
  //val Array(trainData, testData) = splitData(dataWithFeatures)
  //val lgbmClassifier = createLightGBMClassifier()
  //val model = trainModel(trainData, lgbmClassifier)
  //val predictions = makePredictions(model, testData)
  //predictions.select("label", "prediction", "probability").show(10)
  //val rocAuc = evaluateModel(predictions)
  //println(s"ROC-AUC: $rocAuc")

}
