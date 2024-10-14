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

object Main {

  def main(args: Array[String]): Unit = {

    // Importation of the configuration file
    val config = ConfigFactory.parseFile(new File("src/main/resources/config.yaml"))

    // Creation of the SparkSession
    val spark = SparkSession.builder()
      .appName(config.getString("spark.app_name"))
      .master(config.getString("spark.master"))
      .getOrCreate()

    // Création d'un objet pour stocker les durées d'exécution
    val executionTimes = mutable.ArrayBuffer[(String, Double)]()

    // Definition of the paths
    val datapath_flight = config.getString("paths.datapath_flight")
    val datapath_weather = config.getString("paths.datapath_weather")
    val datapath_wban = config.getString("paths.datapath_wban")
    val outputFile_flight = config.getString("paths.output_flight")
    val outputFile_weather = config.getString("paths.output_weather")
    val outputCsv = config.getString("paths.output_csv")

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

    // Reading of the parquet files
    val startReadTime = System.nanoTime()
    val (flight, weather) = readParquetFiles(outputFile_flight, outputFile_weather, spark)
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

    // MANQUE LA BOUCLE POUR TOUT CONFIGURER
    // Creation of the filtered Table
    val (df_delayed_train ,df_delayed_test , df_Ontime_train ,df_Ontime_test)= DF_GenerateFlightDataset(FT_Table, "DS1", 15, 0.0)
    exportDataToCSV(df_delayed_train, outputCsv + "DS1.csv")

    // Jointure des tables FT et OT
    //val FT_Table_prepared = DF_Map(FT_Table, "FT")
    val FT_Table_prepared = DF_Map(df_delayed_train, "FT")
    val OT_Table_prepared = DF_Map(OT_Table, "OT")

    // AJOUTER LES JOINTURES
    val finalDF = DF_Reduce(FT_Table_prepared, OT_Table_prepared)
    exportDataToCSV(finalDF, outputCsv + "Final_df.csv")

    // AJOUTER UN ENREGSITREMENT EN PARQUET
    // RELOAD DES PARQUET

    // Machine Learning
    // Define the feature columns and the label
    val allColumns = finalDF.columns
    val labelCol = "FT_OnTime"
    val featureCols = allColumns.filter(_ != labelCol)

    // Random Forest pipeline
    RandomForest.randomForest(finalDF, labelCol, featureCols)

    // Creation of dataframe from execution times
    val executionTimesDF = createExecutionTimesDataFrame(spark, executionTimes.toSeq)
    exportDataToCSV(executionTimesDF, outputCsv + "execution_times.csv")

    // End of the program
    println("Pipeline Random Forest terminé et résultats sauvegardés.")

  }

}