import org.apache.spark.sql.SparkSession
import Parquet._
import statistics._
import Restatement._
import Library._
import JoinOperations._
import com.typesafe.config.ConfigFactory
import java.io.File
import scala.collection.mutable
import java.nio.file.{Files, Paths}
import org.apache.log4j.Logger

object Main {

  def main(args: Array[String]): Unit = {

    // Importation of the configuration file
    val config = ConfigFactory.parseFile(new File("src/main/resources/config.yaml"))

    // Initialize the logger
    val logger = Logger.getLogger("Main_Logger")

    // Creation of the SparkSession
    val spark = SparkSession.builder()
      .appName(config.getString("spark.app_name"))
      .master(config.getString("spark.master"))
      .getOrCreate()

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
    val (flight, weather) = readParquetFiles(outputFile_flight, outputFile_weather, spark, Statut = true)
    val endReadTime = System.nanoTime()
    val durationReadTime = (endReadTime - startReadTime) / 1e9d
    executionTimes += (("read_parquet", durationReadTime))

    // WBAN dataframe creation
    val wban = spark.read.format("csv").option("header", "true").option("delimiter", ",").load(datapath_wban)

    // Quality data statistics
    val flight_MissingValue = count_missing_values(flight, spark)
    val weather_MissingValue = count_missing_values(weather, spark)
    val countSinFlag = countFlagsWithSAndExport(weather)
    flight_MissingValue.write.mode("overwrite").csv(outputCsv + "flight_missingValues.csv")
    weather_MissingValue.write.mode("overwrite").csv(outputCsv + "weather_missingValues.csv")
    countSinFlag.write.mode("overwrite").csv(outputCsv + "countSinFlag.csv")

    // Calculate descriptive statistics
    val countAirport = statistics.countAirport(flight)
    val countCarrier = statistics.countCarrier(flight, 15)
    val countDelayedFlight = statistics.countDelayedFlight(flight)
    countAirport.write.mode("overwrite").csv(outputCsv + "descriptive_stats.csv")
    countCarrier.write.mode("overwrite").csv(outputCsv + "carrier_stats.csv")
    countDelayedFlight.write.mode("overwrite").csv(outputCsv + "delayed_stats.csv")

    // Creation of the FT and OT tables
    val startCreationTableTime = System.nanoTime()
    val FT_Table = createFlightTable(flight, wban)
    val OT_Table = createObservationTable(weather, wban, 0.20)
    val endCreationTableTime = System.nanoTime()
    val durationCreationTableime = (endCreationTableTime - startCreationTableTime) / 1e9d
    executionTimes += (("Create_tables", durationCreationTableime))

    // Export des tables FT et OT
    // UNIQUEMENT POUR LES TESTS --> A SUPPRIMER
    exportDataToCSV(FT_Table, outputCsv + "FT_Table.csv")
    exportDataToCSV(OT_Table, outputCsv + "OT_Table.csv")

    // First step for the join of the tables
    val startJoinFirstStepTime = System.nanoTime()
    val FT_Table_prepared = DF_Map(FT_Table, "FT")
    val OT_Table_prepared = DF_Map(OT_Table, "OT")
    val endJoinFirstStepTime = System.nanoTime()
    val durationJoinFirstStepTime = (endJoinFirstStepTime - startJoinFirstStepTime) / 1e9d
    executionTimes += (("Join_tables_first_step", durationJoinFirstStepTime))

    // Get the cluster information
    val (partitionsBasedOnCoresMin, partitionsBasedOnCoresMax, partitionsBasedOnSizeMin, partitionsBasedOnSizeMax) = getClusterInfo(FT_Table_prepared, spark.sparkContext, spark)

    // Second step for the join of the tables
    val startJoinSecondStepInColumnTime = System.nanoTime()
    val finalDF_Cols = DF_Reduce_Cols(FT_Table_prepared, OT_Table_prepared, partitionsBasedOnCoresMin)
    exportDataToCSV(finalDF_Cols, outputCsv + "finalDF_C.csv")
    val endJoinSecondStepInColumnTime = System.nanoTime()
    val durationJoinSecondStepInColumnTime = (endJoinSecondStepInColumnTime - startJoinSecondStepInColumnTime) / 1e9d
    executionTimes += (("Join_tables_second_step_column", durationJoinSecondStepInColumnTime))

    val startJoinSecondStepInLineTime = System.nanoTime()
    val finalDF_Lines = DF_Reduce_Line(FT_Table_prepared, OT_Table_prepared, spark)
    exportDataToCSV(finalDF_Lines, outputCsv + "finalDF_L.csv")
    val endJoinSecondStepInLineTime = System.nanoTime()
    val durationJoinSecondStepInLineTime = (endJoinSecondStepInLineTime - startJoinSecondStepInLineTime) / 1e9d
    executionTimes += (("Join_tables_second_step_line", durationJoinSecondStepInLineTime))

    // Store the final dataframes in parquet format
    val startStoreParquetFinalDfTime = System.nanoTime()
    storeParquetFiles(finalDF_Cols, outputFinal_Cols_Parquet)
    storeParquetFiles(finalDF_Lines, outputFinal_Lines_Parquet)
    val endStoreParquetFinalDfTime = System.nanoTime()
    val durationStoreParquetFinalDfTime = (endStoreParquetFinalDfTime - startStoreParquetFinalDfTime) / 1e9d
    executionTimes += (("Store_parquet_FinalDf", durationStoreParquetFinalDfTime))

    // Reload of the parquet files from final dataframes
    val startReloadParquetFinalDfTime = System.nanoTime()
    val (finalDF_Cols_Reloaded, finalDF_Lines_Reloaded) = readParquetFiles(outputFinal_Cols_Parquet, outputFinal_Lines_Parquet, spark, Statut= false)
    val endReloadParquetFinalDfTime = System.nanoTime()
    val durationReloadParquetFinalDfTime = (endReloadParquetFinalDfTime - startReloadParquetFinalDfTime) / 1e9d
    executionTimes += (("Reload_parquet_FinalDf", durationReloadParquetFinalDfTime))


    // Creation of the filtered Table
    //val (df_delayed_train ,df_delayed_test , df_Ontime_train ,df_Ontime_test)= DF_GenerateFlightDataset(FT_Table, "DS1", 15, 0.0)
    //exportDataToCSV(df_delayed_train, outputCsv + "DS1.csv")



    // Machine Learning
    // Define the feature columns and the label
    //val allColumns = finalDF.columns
    //val labelCol = "FT_OnTime"
    //val featureCols = allColumns.filter(_ != labelCol)

    // Random Forest pipeline
    //RandomForest.randomForest(finalDF, labelCol, featureCols)

    // IMPLEMENTATION d'UN DEUXI7ME MODELE DE ML ??

    // Creation of dataframe from execution times
    val executionTimesDF = createExecutionTimesDataFrame(spark, executionTimes.toSeq)
    exportDataToCSV(executionTimesDF, outputCsv + "execution_times.csv")

    // End of the program
    //println("Pipeline Random Forest terminé et résultats sauvegardés.")

  }

}