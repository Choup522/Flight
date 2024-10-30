import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import Parquet._
import statistics._
import Restatement._
import Library._
import JoinOperations._
import RandomForest.saveMetricsAsCSV
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger

import java.io.File
import scala.collection.mutable
import java.nio.file.{Files, Paths}

object Main {

  def main(args: Array[String]): Unit = {

    // Initialize the logger
    val logger = Logger.getLogger("Main_Logger")

    // Importation of the configuration file
    val config = ConfigFactory.parseFile(new File("src/main/resources/config.yaml"))

    // Creation of the SparkSession
    val spark = SparkSession.builder()
      .appName(config.getString("spark.app_name"))
      .master(config.getString("spark.master"))
      .getOrCreate()

    // Check if we need to reload and train
    val reloadAndTrain = config.getString("execution.reload_and_train")

    // Creation of the execution times array
    val executionTimes = mutable.ArrayBuffer[(String, Double)]()

    // Definition of the paths
    val datapath_flight = config.getString("paths.datapath_flight")
    val datapath_weather = config.getString("paths.datapath_weather")
    val datapath_wban = config.getString("paths.datapath_wban")
    val outputFile_flight = config.getString("paths.output_flight")
    val outputFile_weather = config.getString("paths.output_weather")
    val outputCsv = config.getString("paths.output_csv")
    val outputFinal_Cols_Parquet = config.getString("paths.output_final_Col_parquet")
    val outputFinal_Lines_Parquet = config.getString("paths.output_final_line_parquet")

    reloadAndTrain match {

      case "BigData" =>
        logger.info("BigData mode")
        prepareAndLoadData(spark, datapath_flight, datapath_weather, datapath_wban, outputFile_flight, outputFile_weather, outputCsv, outputFinal_Cols_Parquet, outputFinal_Lines_Parquet, executionTimes)

      case "MachineLearning" =>
        logger.info("Machine Learning mode")
        reloadAndGenerateDatasetsAndTrain(spark, outputFinal_Cols_Parquet, outputFinal_Lines_Parquet, outputCsv, executionTimes)

      case "All" =>
        logger.info("All mode")
        prepareAndLoadData(spark, datapath_flight, datapath_weather, datapath_wban, outputFile_flight, outputFile_weather, outputCsv, outputFinal_Cols_Parquet, outputFinal_Lines_Parquet, executionTimes)
        reloadAndGenerateDatasetsAndTrain(spark, outputFinal_Cols_Parquet, outputFinal_Lines_Parquet, outputCsv, executionTimes)

      case _ =>
        logger.error("Invalid mode")
    }

    // Creation of dataframe from execution times
    val executionTimesDF = createExecutionTimesDataFrame(spark, executionTimes.toSeq)
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

  private def prepareAndLoadData(spark: SparkSession, datapath_flight: String, datapath_weather: String, datapath_wban: String, outputFile_flight: String, outputFile_weather: String, outputCsv: String, outputFinal_Cols_Parquet: String, outputFinal_Lines_Parquet: String, executionTimes: mutable.ArrayBuffer[(String, Double)]): Unit = {

    // Création of parquet files
    val flightParquetExists = Files.exists(Paths.get(outputFile_flight))
    val weatherParquetExists = Files.exists(Paths.get(outputFile_weather))

    if (!flightParquetExists || !weatherParquetExists) {
      println("Parquet Files not found. Creation of Parquet files.")
      val startCreateParquetTime = System.nanoTime()
      createParquetFile(datapath_flight, datapath_weather, outputFile_flight , outputFile_weather, spark, sample = true)
      val endCreateParquetTime = System.nanoTime()
      val durationCreateParquetTime = (endCreateParquetTime - startCreateParquetTime) / 1e9d
      executionTimes += (("create_parquet", durationCreateParquetTime))
    } else {
      println("Parquet files found.")
    }

    // Load the dataframes from the parquet files
    val startReadTime = System.nanoTime()
    val (flight, weather, statsDF) = readParquetFiles(outputFile_flight, outputFile_weather, spark, Status = true)
    Library.exportDataToCSV(statsDF, outputCsv + "parquet_files_stats_1.csv")
    val endReadTime = System.nanoTime()
    val durationReadTime = (endReadTime - startReadTime) / 1e9d
    executionTimes += (("read_parquet", durationReadTime))

    // WBAN dataframe creation
    val wban = spark.read.format("csv").option("header", "true").option("delimiter", ",").load(datapath_wban)

    // Quality data statistics
    val flight_MissingValue = count_missing_values(flight, spark)
    val weather_MissingValue = count_missing_values(weather, spark)
    val countSinFlag = countFlagsWithSAndExport(weather)
    Library.exportDataToCSV(flight_MissingValue, outputCsv + "flight_missingValues.csv")
    Library.exportDataToCSV(weather_MissingValue, outputCsv + "weather_missingValues.csv")
    Library.exportDataToCSV(countSinFlag, outputCsv + "countSinFlag.csv")

    // Calculate descriptive statistics
    val countAirport = statistics.countAirport(flight)
    val countCarrier = statistics.countCarrier(flight, 15)
    val countDelayedFlight = statistics.countDelayedFlight(flight)
    Library.exportDataToCSV(countAirport, outputCsv + "descriptive_stats.csv")
    Library.exportDataToCSV(countCarrier, outputCsv + "carrier_stats.csv")
    Library.exportDataToCSV(countDelayedFlight, outputCsv + "delayed_stats.csv")

    // Creation of the FT and OT tables
    val startCreationTableTime = System.nanoTime()
    val FT_Table = createFlightTable(flight, wban)
    val OT_Table = createObservationTable(weather, wban, 0.20)
    val endCreationTableTime = System.nanoTime()
    val durationCreationTableTime = (endCreationTableTime - startCreationTableTime) / 1e9d
    executionTimes += (("Create_tables", durationCreationTableTime))

    // First step for the join of the tables
    val startJoinFirstStepTime = System.nanoTime()
    val FT_Table_prepared = DF_Map(FT_Table, "FT")
    val OT_Table_prepared = DF_Map(OT_Table, "OT")
    val endJoinFirstStepTime = System.nanoTime()
    val durationJoinFirstStepTime = (endJoinFirstStepTime - startJoinFirstStepTime) / 1e9d
    executionTimes += (("Join_tables_first_step", durationJoinFirstStepTime))

    // Get the cluster information
    val partitionsBasedOnCores = getClusterInfo(spark.sparkContext)

    // Second step for the join of the tables
    val startJoinSecondStepInColumnTime = System.nanoTime()
    val finalDF_Cols = DF_Reduce_Cols(FT_Table_prepared, OT_Table_prepared, partitionsBasedOnCores)
    val endJoinSecondStepInColumnTime = System.nanoTime()
    val durationJoinSecondStepInColumnTime = (endJoinSecondStepInColumnTime - startJoinSecondStepInColumnTime) / 1e9d
    executionTimes += (("Join_tables_second_step_column", durationJoinSecondStepInColumnTime))

    val startJoinSecondStepInLineTime = System.nanoTime()
    val finalDF_Lines = DF_Reduce_Line(FT_Table_prepared, OT_Table_prepared, spark)
    val endJoinSecondStepInLineTime = System.nanoTime()
    val durationJoinSecondStepInLineTime = (endJoinSecondStepInLineTime - startJoinSecondStepInLineTime) / 1e9d
    executionTimes += (("Join_tables_second_step_line", durationJoinSecondStepInLineTime))

    // Store the final dataframes in parquet format
    val startStoreParquetFinalDfTime = System.nanoTime()
    storeParquetFiles(finalDF_Cols, outputFinal_Cols_Parquet, Seq("FT_Year", "FT_FL_DATE"), partitionsBasedOnCores)
    storeParquetFiles(finalDF_Lines, outputFinal_Lines_Parquet, Seq("FT_Year", "FT_FL_DATE"), partitionsBasedOnCores)
    val endStoreParquetFinalDfTime = System.nanoTime()
    val durationStoreParquetFinalDfTime = (endStoreParquetFinalDfTime - startStoreParquetFinalDfTime) / 1e9d
    executionTimes += (("Store_parquet_FinalDf", durationStoreParquetFinalDfTime))

    // Export of the schema of the final dataframes
    exportSchema(finalDF_Cols, outputCsv + "schema_finalDF_Cols.json")
    exportSchema(finalDF_Lines, outputCsv + "schema_finalDF_Lines.json")

  }

  private def reloadAndGenerateDatasetsAndTrain(spark: SparkSession, outputFinal_Cols_Parquet: String, outputFinal_Lines_Parquet: String, outputCsv: String, executionTimes: mutable.ArrayBuffer[(String, Double)]): Unit = {

    // Reload of the parquet files from final dataframes
    val startReloadParquetFinalDfTime = System.nanoTime()
    val (finalDF_Cols_Reloaded, finalDF_Lines_Reloaded, statsDF_2) = readParquetFiles(outputFinal_Cols_Parquet, outputFinal_Lines_Parquet, spark, Status = false)
    Library.exportDataToCSV(statsDF_2, outputCsv + "parquet_files_stats_2.csv")
    val endReloadParquetFinalDfTime = System.nanoTime()
    val durationReloadParquetFinalDfTime = (endReloadParquetFinalDfTime - startReloadParquetFinalDfTime) / 1e9d
    executionTimes += (("Reload_parquet_FinalDf", durationReloadParquetFinalDfTime))

    // Generation of filtered datasets
    val startCreationOfFilteredDataframeTime = System.nanoTime()
    val intervals = List(15, 30) // Interval of time in minutes

    // Structure to store the generated dataframes
    val datasets = scala.collection.mutable.Map[String, DataFrame]()

    // Iteration to generate DS1, DS2, etc. with a variable time interval
    intervals.zipWithIndex.foreach { case (interval, idx) =>
      val dsName = s"DS${idx + 1}"

      // Generation of the datasets for the dataframes in columns
      val (df_delayed_train_Cols, df_delayed_test_Cols, df_Ontime_train_Cols, df_Ontime_test_Cols) = DF_GenerateFlightDataset(finalDF_Cols_Reloaded, dsName, interval, 0.0)

      // Stock of the dataframes in a Map with unique keys
      datasets(s"${dsName}_Cols_${interval}min_delayed_train") = df_delayed_train_Cols
      datasets(s"${dsName}_Cols_${interval}min_delayed_test") = df_delayed_test_Cols
      datasets(s"${dsName}_Cols_${interval}min_ontime_train") = df_Ontime_train_Cols
      datasets(s"${dsName}_Cols_${interval}min_ontime_test") = df_Ontime_test_Cols

      // Generation of the datasets for the dataframes in lines
      val (df_delayed_train_Lines, df_delayed_test_Lines, df_Ontime_train_Lines, df_Ontime_test_Lines) = DF_GenerateFlightDataset(finalDF_Lines_Reloaded, dsName, interval, 0.0)

      // Stock of the dataframes in a Map with unique keys
      datasets(s"${dsName}_Lines_${interval}min_delayed_train") = df_delayed_train_Lines
      datasets(s"${dsName}_Lines_${interval}min_delayed_test") = df_delayed_test_Lines
      datasets(s"${dsName}_Lines_${interval}min_ontime_train") = df_Ontime_train_Lines
      datasets(s"${dsName}_Lines_${interval}min_ontime_test") = df_Ontime_test_Lines
    }

    val endCreationOfFilteredDataframeTime = System.nanoTime()
    val durationCreationOfFilteredDataframeTime = (endCreationOfFilteredDataframeTime - startCreationOfFilteredDataframeTime) / 1e9d
    executionTimes += (("FilteredDataFrames", durationCreationOfFilteredDataframeTime))

    // Machine Learning
    // Define the feature columns and the label
    val startRandomForestTime = System.nanoTime()
    val labelCol = "FT_OnTime"
    val featureCols_1 = finalDF_Cols_Reloaded.columns.filter(_ != labelCol)
    val featureCols_2 = finalDF_Lines_Reloaded.columns.filter(_ != labelCol)

    // Random Forest pipeline - Loop through the datasets and train the model for each one
    intervals.zipWithIndex.foreach { case (interval, idx) =>

      val dsName = s"DS${idx + 1}"

      // Train and evaluate for datasets
      val metrics_1 = RandomForest.randomForest(datasets(s"${dsName}_Cols_${interval}min_delayed_train"), labelCol, featureCols_1)
      val metrics_2 = RandomForest.randomForest(datasets(s"${dsName}_Lines_${interval}min_delayed_train"), labelCol, featureCols_2)

      // Save metrics in CSV format
      saveMetricsAsCSV(spark, metrics_1, outputCsv + "randomForestMetrics_1")
      saveMetricsAsCSV(spark, metrics_2, outputCsv + "randomForestMetrics_2")

      val endRandomForestTime = System.nanoTime()
      val durationRandomForestTime = (endRandomForestTime - startRandomForestTime) / 1e9d
      executionTimes += (("RandomForest", durationRandomForestTime))
    }
  }

}