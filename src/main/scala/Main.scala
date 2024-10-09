import org.apache.spark.sql.SparkSession
import Parquet._
import statistics._
import Restatement._
import Library._
import JoinOperations._
import com.typesafe.config.ConfigFactory
import java.io.File

object Main {

  def main(args: Array[String]): Unit = {

    // Importation of the configuration file
    val config = ConfigFactory.parseFile(new File("src/main/resources/config.yaml"))

    // Creation of the SparkSession
    val spark = SparkSession.builder()
      .appName(config.getString("spark.app_name"))
      .master(config.getString("spark.master"))
      .getOrCreate()

    // Definition of the paths
    val datapath_flight = config.getString("paths.datapath_flight")
    val datapath_weather = config.getString("paths.datapath_weather")
    val datapath_wban = config.getString("paths.datapath_wban")
    val outputFile_flight = config.getString("paths.output_flight")
    val outputFile_weather = config.getString("paths.output_weather")
    val outputCsv = config.getString("paths.output_csv")

    //createParquetFile(datapath_flight, datapath_weather, outputFile_flight , outputFile_weather, spark, sample = true)

    // Reading of the parquet files
    val (flight, weather) = readParquetFiles(outputFile_flight, outputFile_weather, spark)

    // WBAN dataframe creation
    val wban = spark.read.format("csv").option("header", "true").option("delimiter", ",").load(datapath_wban)

    // Quality data statistics
    val flight_MissingValue = count_missing_values(flight, spark)
    val weather_MissingValue = count_missing_values(weather, spark)
    val countSinFlag = countFlagsWithSAndExport(weather)
    flight_MissingValue.write.mode("overwrite").csv(outputCsv + "flight_missingValues.csv")
    weather_MissingValue.write.mode("overwrite").csv(outputCsv + "weather_missingValues.csv")
    countSinFlag.write.mode("overwrite").csv(outputCsv + "countSinFlag.csv")

    // Creation of the FT and OT tables
    val FT_Table = createFlightTable(flight, wban)
    val OT_Table = createObservationTable(weather, wban, 0.20)

    // Export des tables FT et OT
    // UNIQUEMENT POUR LES TESTS --> A SUPPRIMER
    exportDataToCSV(FT_Table, outputCsv + "FT_Table.csv")
    exportDataToCSV(OT_Table, outputCsv + "OT_Table.csv")

    // Jointure des tables FT et OT
    val FT_Table_prepared = DF_Map(FT_Table, "FT")
    val OT_Table_prepared = DF_Map(OT_Table, "OT")

    // AJOUTER LES JOINTURES
    val finalDF = DF_Reduce(FT_Table_prepared, OT_Table_prepared)
    exportDataToCSV(finalDF, outputCsv + "Final_df.csv")

    // Calculate descriptive statistics
    val countAirport = statistics.countAirport(flight)
    val countCarrier = statistics.countCarrier(flight, 15)
    val countDelayedFlight = statistics.countDelayedFlight(flight)
    countAirport.write.mode("overwrite").csv(outputCsv + "descriptive_stats.csv")
    countCarrier.write.mode("overwrite").csv(outputCsv + "carrier_stats.csv")
    countDelayedFlight.write.mode("overwrite").csv(outputCsv + "delayed_stats.csv")
  }

}