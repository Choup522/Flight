import org.apache.spark.sql.{DataFrame, SparkSession}
import Parquet._
import statistics._
import Restatement._
import Library._
import JoinOperations._
import RandomForest.saveMetricsAsCSV
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import scala.collection.mutable
import java.nio.file.{Files, Paths}
import com.typesafe.config.Config
import LoggerFactory.logger

// Creation of the logger object
object LoggerFactory {
  val logger: Logger = Logger.getLogger("Main_Logger")
}

object Main {

  def main(args: Array[String]): Unit = {

    // Load the JSON configuration file using ConfigFactory
    val config: Config = ConfigFactory.load("config") // Assumes the file is named config.json and in the classpath
    if (config.isEmpty) {
      throw new RuntimeException("Configuration file 'config.json' not found or is empty in the classpath.")
    }

    // Initialize the logger
    logger.info("Starting the application")

    // Get the environment
    val env = config.getString("execution.environment")

    val paths = if (env == "local") {
      config.getConfig("paths.local")
    } else {
      config.getConfig("paths.hdfs")
    }

    // Creation of the spark session
    val spark = SparkSession.builder()
      .appName(config.getString("spark.app_name"))
      .master(config.getString("spark.master"))
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "4g")
      .config("spark.executor.cores", "2")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.network.timeout", "300s")
      .config("spark.executor.heartbeatInterval", "60s")
      .getOrCreate()

    logger.info("Spark session created")

    //spark.sparkContext.setLogLevel("ERROR")

    // Check if we need to reload and train
    val reloadAndTrain = config.getString("execution.reload_and_train")

    // Initialize the generation mode
    val generationMode = config.getString("execution.generation_mode")

    // Initialization of the value for sample
    val sample = config.getBoolean("execution.sample")
    val fraction = config.getDouble("execution.fraction")
    val reloadParquet = config.getBoolean("execution.reload_parquet")

    // Creation of the execution times array
    val executionTimes = mutable.ArrayBuffer[(String, Double)]()

    // Get the paths from the configuration file
    val datapath_flight = paths.getString("datapath_flight")
    val datapath_weather = paths.getString("datapath_weather")
    val datapath_wban = paths.getString("datapath_wban")
    val outputFile_flight = paths.getString("output_flight")
    val outputFile_weather = paths.getString("output_weather")
    val outputCsv = paths.getString("output_csv")
    val outputFinal_Cols_Parquet = paths.getString("output_final_Col_parquet")
    val outputFinal_Lines_Parquet = paths.getString("output_final_line_parquet")

    reloadAndTrain match {

      case "BigData" =>
        logger.info("BigData mode")
        prepareAndLoadData(spark, datapath_flight, datapath_weather, datapath_wban, outputFile_flight, outputFile_weather, outputCsv, outputFinal_Cols_Parquet, outputFinal_Lines_Parquet, executionTimes, sample, fraction)

      case "MachineLearning" =>
        logger.info("Machine Learning mode")
        reloadAndGenerateDatasetsAndTrain(spark, outputCsv, executionTimes, generationMode, spark.emptyDataFrame, spark.emptyDataFrame)

      case "All" =>
        logger.info("All mode")
        val (finalDF_Cols, finalDF_Lines) = prepareAndLoadData(spark, datapath_flight, datapath_weather, datapath_wban, outputFile_flight, outputFile_weather, outputCsv, outputFinal_Cols_Parquet, outputFinal_Lines_Parquet, executionTimes, sample, fraction)
        reloadAndGenerateDatasetsAndTrain(spark, outputCsv, executionTimes, generationMode, finalDF_Cols, finalDF_Lines)

      case _ =>
        logger.error("Invalid mode")
    }

    // Creation of dataframe from execution times
    val executionTimesDF = createExecutionTimesDataFrame(spark, executionTimes.toIndexedSeq)
    exportDataToCSV(executionTimesDF, outputCsv + "execution_times.csv")

    // End of the program
    spark.close()

    reloadAndTrain match {
      case "BigData" =>
        logger.info("BigData mode done")
      case "MachineLearning" =>
        logger.info("Machine Learning mode done")
      case "All" =>
        logger.info("All program done")

    }

  }

  private def prepareAndLoadData(spark: SparkSession, datapath_flight: String, datapath_weather: String, datapath_wban: String, outputFile_flight: String, outputFile_weather: String, outputCsv: String, outputFinal_Cols_Parquet: String, outputFinal_Lines_Parquet: String, executionTimes: mutable.ArrayBuffer[(String, Double)], sample: Boolean, fraction: Double): (DataFrame, DataFrame) = {

    // Création of parquet files
    val flightParquetExists = Files.exists(Paths.get(outputFile_flight))
    val weatherParquetExists = Files.exists(Paths.get(outputFile_weather))

    if (!flightParquetExists || !weatherParquetExists) {

      // Variables to define the sample mode
      logger.info("Parquet Files not found. Creation of Parquet files.")
      val startCreateParquetTime = System.nanoTime()
      createParquetFile(datapath_flight, datapath_weather, outputFile_flight , outputFile_weather, spark, sample, fraction)
      val endCreateParquetTime = System.nanoTime()
      val durationCreateParquetTime = (endCreateParquetTime - startCreateParquetTime) / 1e9d
      executionTimes += (("create_parquet", durationCreateParquetTime))
    } else {
      logger.info("Parquet Files found.")
    }

    // Load the dataframes from the parquet files
    logger.info("Reading the parquet files")
    val startReadTime = System.nanoTime()
    val (flight, weather, statsDF) = readParquetFiles(outputFile_flight, outputFile_weather, outputCsv, spark, Status = true)
    Library.exportDataToCSV(statsDF, outputCsv + "parquet_files_stats_1.csv")
    val endReadTime = System.nanoTime()
    val durationReadTime = (endReadTime - startReadTime) / 1e9d
    executionTimes += (("read_parquet", durationReadTime))

    // WBAN dataframe creation
    logger.info("Creating the WBAN dataframe")
    val wban = spark.read.format("csv").option("header", "true").option("delimiter", ",").load(datapath_wban)

    // Quality data statistics
    logger.info("Quality data statistics")
    val flight_MissingValue = count_missing_values(flight, spark)
    val weather_MissingValue = count_missing_values(weather, spark)
    val countSinFlag = countFlagsWithSAndExport(weather)
    Library.exportDataToCSV(flight_MissingValue, outputCsv + "flight_missingValues.csv")
    Library.exportDataToCSV(weather_MissingValue, outputCsv + "weather_missingValues.csv")
    Library.exportDataToCSV(countSinFlag, outputCsv + "countSinFlag.csv")

    // Calculate descriptive statistics
    logger.info("Descriptive statistics")
    val countAirport = statistics.countAirport(flight)
    val countCarrier = statistics.countCarrier(flight, 15)
    val countDelayedFlight = statistics.countDelayedFlight(flight)
    Library.exportDataToCSV(countAirport, outputCsv + "descriptive_stats.csv")
    Library.exportDataToCSV(countCarrier, outputCsv + "carrier_stats.csv")
    Library.exportDataToCSV(countDelayedFlight, outputCsv + "delayed_stats.csv")

    // Creation of the FT and OT tables
    logger.info("Creation of the FT and OT tables")
    val startCreationTableTime = System.nanoTime()
    val FT_Table = createFlightTable(flight, wban)
    val OT_Table = createObservationTable(weather, wban, 0.20)
    val endCreationTableTime = System.nanoTime()
    val durationCreationTableTime = (endCreationTableTime - startCreationTableTime) / 1e9d
    executionTimes += (("Create_tables", durationCreationTableTime))

    // First step for the join of the tables
    logger.info("Join of the tables")
    val startJoinFirstStepTime = System.nanoTime()
    val FT_Table_prepared = DF_Map(FT_Table, "FT")
    val OT_Table_prepared = DF_Map(OT_Table, "OT")
    val endJoinFirstStepTime = System.nanoTime()
    val durationJoinFirstStepTime = (endJoinFirstStepTime - startJoinFirstStepTime) / 1e9d
    executionTimes += (("Join_tables_first_step", durationJoinFirstStepTime))

    // Second step for the join of the tables
    logger.info("Second step for the join of the tables")
    val startJoinSecondStepInColumnTime = System.nanoTime()
    val finalDF_Cols = DF_Reduce_Cols(FT_Table_prepared, OT_Table_prepared)
    val endJoinSecondStepInColumnTime = System.nanoTime()
    val durationJoinSecondStepInColumnTime = (endJoinSecondStepInColumnTime - startJoinSecondStepInColumnTime) / 1e9d
    executionTimes += (("Join_tables_second_step_column", durationJoinSecondStepInColumnTime))

    val startJoinSecondStepInLineTime = System.nanoTime()
    val finalDF_Lines = DF_Reduce_Line(FT_Table_prepared, OT_Table_prepared, spark)
    val endJoinSecondStepInLineTime = System.nanoTime()
    val durationJoinSecondStepInLineTime = (endJoinSecondStepInLineTime - startJoinSecondStepInLineTime) / 1e9d
    executionTimes += (("Join_tables_second_step_line", durationJoinSecondStepInLineTime))

    // Store the final dataframes in parquet format
    logger.info("Store the final dataframes in parquet format")
    val startStoreParquetFinalDfTime = System.nanoTime()
    storeParquetFiles(finalDF_Cols, outputFinal_Cols_Parquet, Seq("FT_Year", "FT_FL_DATE"))
    storeParquetFiles(finalDF_Lines, outputFinal_Lines_Parquet, Seq("FT_Year", "FT_FL_DATE"))
    val endStoreParquetFinalDfTime = System.nanoTime()
    val durationStoreParquetFinalDfTime = (endStoreParquetFinalDfTime - startStoreParquetFinalDfTime) / 1e9d
    executionTimes += (("Store_parquet_FinalDf", durationStoreParquetFinalDfTime))

    // Export of the schema of the final dataframes
    logger.info("Export of the schema of the final dataframes")
    exportSchema(finalDF_Cols, outputCsv + "schema_finalDF_Cols.json")
    exportSchema(finalDF_Lines, outputCsv + "schema_finalDF_Lines.json")

    // Validate the dataframes
    try {
      require(!finalDF_Cols.isEmpty, "Le DataFrame est vide.")
    } catch {
      case e: IllegalArgumentException =>
        logger.error(s"Validation Failed : ${e.getMessage}")
    }

    (finalDF_Cols, finalDF_Lines)
  }

  private def reloadAndGenerateDatasetsAndTrain(spark: SparkSession, outputCsv: String, executionTimes: mutable.ArrayBuffer[(String, Double)], generationMode: String = "cols", DataframeCols : DataFrame, DataFrameLines: DataFrame): Unit = {

    logger.info("Using dataframes already created")
    val finalDF_Cols_Reloaded = if (generationMode == "cols" || generationMode == "both") DataframeCols else spark.emptyDataFrame
    val finalDF_Lines_Reloaded = if (generationMode == "lines" || generationMode == "both") DataFrameLines else spark.emptyDataFrame
    //}

    // Generation of filtered datasets
    logger.info("Creation of the filtered DataSet DS1")
    val startCreationOfFilteredDataframeTime = System.nanoTime()
    val interval = 15
    val dsName = "DS1"

    val datasets: Map[String, DataFrame] = if (generationMode == "cols" || generationMode == "both") {
      val (df_delayed_train_Cols, df_delayed_test_Cols, df_Ontime_train_Cols, df_Ontime_test_Cols) =
        DF_GenerateFlightDataset(spark, finalDF_Cols_Reloaded, dsName, interval, 1.0)

      Map(
        s"${dsName}_Cols_${interval}min_delayed_train" -> df_delayed_train_Cols,
        s"${dsName}_Cols_${interval}min_delayed_test" -> df_delayed_test_Cols,
        s"${dsName}_Cols_${interval}min_ontime_train" -> df_Ontime_train_Cols,
        s"${dsName}_Cols_${interval}min_ontime_test" -> df_Ontime_test_Cols
      )
    } else if (generationMode == "lines" || generationMode == "both") {
      val (df_delayed_train_Lines, df_delayed_test_Lines, df_Ontime_train_Lines, df_Ontime_test_Lines) =
        DF_GenerateFlightDataset(spark, finalDF_Lines_Reloaded, dsName, interval, 1.0)

      Map(
        s"${dsName}_Lines_${interval}min_delayed_train" -> df_delayed_train_Lines,
        s"${dsName}_Lines_${interval}min_delayed_test" -> df_delayed_test_Lines,
        s"${dsName}_Lines_${interval}min_ontime_train" -> df_Ontime_train_Lines,
        s"${dsName}_Lines_${interval}min_ontime_test" -> df_Ontime_test_Lines
      )
    } else Map.empty[String, DataFrame]

    val endCreationOfFilteredDataframeTime = System.nanoTime()
    val durationCreationOfFilteredDataframeTime = (endCreationOfFilteredDataframeTime - startCreationOfFilteredDataframeTime) / 1e9d
    executionTimes += (("FilteredDataFrames", durationCreationOfFilteredDataframeTime))

    // Machine Learning
    // Define the feature columns and the label
    logger.info("Machine Learning phase")
    val startMLTime = System.nanoTime()
    val labelCol = "FT_OnTime"

    if (generationMode == "cols" || generationMode == "both") {
      val featureCols = finalDF_Cols_Reloaded.columns.filter(_ != labelCol)
      val reducedColsDataset = datasets(s"${dsName}_Cols_${interval}min_delayed_train")
        .select(labelCol, featureCols: _*)
        .repartition(100)

      val metricsCols = RandomForest.randomForest(reducedColsDataset, labelCol, featureCols, 20, 5, 3)
      saveMetricsAsCSV(spark, metricsCols, s"$outputCsv/metrics_${dsName}_Cols.csv")
    }

    if (generationMode == "lines" || generationMode == "both") {
      val featureCols = finalDF_Lines_Reloaded.columns.filter(_ != labelCol)
      val reducedLinesDataset = datasets(s"${dsName}_Lines_${interval}min_delayed_train")
        .select(labelCol, featureCols: _*)
        .repartition(100)

      val metricsLines = RandomForest.randomForest(reducedLinesDataset, labelCol, featureCols, 20, 5, 5)
      saveMetricsAsCSV(spark, metricsLines, s"$outputCsv/metrics_${dsName}_Lines.csv")
    }

    val endMLTime = System.nanoTime()
    val durationMLTime = (endMLTime - startMLTime) / 1e9d
    executionTimes += (("RandomForest", durationMLTime))
  }
}