import org.apache.spark.sql.SparkSession
import Parquet._
import Statistiques._
import Retraitements._
import com.typesafe.config.ConfigFactory

import java.io.File

object Main {

  def main(args: Array[String]): Unit = {

    // Import de la configuration
    val config = ConfigFactory.parseFile(new File("src/main/resources/config.yaml"))

    // Création de la session Spark
    val spark = SparkSession.builder()
      .appName(config.getString("spark.app_name"))
      .master(config.getString("spark.master"))
      .getOrCreate()

    // Défintion des chemins des fichiers
    val datapath_flight = config.getString("paths.datapath_flight")
    val datapath_weather = config.getString("paths.datapath_weather")
    val outputFile_flight = config.getString("paths.output_flight")
    val outputFile_weather = config.getString("paths.output_weather")
    val outputCsv = config.getString("paths.output_csv")

    //createParquetFile(datapath_flight, datapath_weather, outputFile_flight , outputFile_weather, spark, sample = true)

    //Lecture des fichiers parquet
    val (flight, weather) = readParquetFiles(outputFile_flight, outputFile_weather, spark)

    // Statistiques sur les qualités des données
    val flight_MissingValue = count_missing_values(flight, spark)
    val weather_MissingValue = count_missing_values(weather, spark)
    val countSinFlag = countFlagsWithSAndExport(weather)
    flight_MissingValue.write.mode("overwrite").csv(outputCsv + "flight_missingValues.csv")
    weather_MissingValue.write.mode("overwrite").csv(outputCsv + "weather_missingValues.csv")
    countSinFlag.write.mode("overwrite").csv(outputCsv + "countSinFlag.csv")

    // Retraitement des données
    val FT_Table = createFlightTable(flight)
    //val OT_Table = createObservationTable(weather, flight)

    // Jointure des tables FT et OT


    // Calcul des statistiques
    val countAirport = Statistiques.countAirport(flight)
    val countCarrier = Statistiques.countCarrier(flight, 15)
    val countDelayedFlight = Statistiques.countDelayedFlight(flight)
    countAirport.write.mode("overwrite").csv(outputCsv + "descriptive_stats.csv")
    countCarrier.write.mode("overwrite").csv(outputCsv + "carrier_stats.csv")
    countDelayedFlight.write.mode("overwrite").csv(outputCsv + "delayed_stats.csv")
  }

}