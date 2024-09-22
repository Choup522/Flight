import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Parquet {

  def createParquetFile(datapath_flight: String, datapath_weather: String, outputFile_flight: String, outputFile_weather: String, spark: SparkSession, sample: Boolean): Unit = {

    //Schéma de la table Flight
    val flightSchema = StructType(Array(
      StructField("OP_CARRIER_AIRLINE_ID", StringType, nullable = true),   // ID de la compagnie aérienne
      StructField("OP_CARRIER_FL_NUM", StringType, nullable = true),       // Numéro de vol de la compagnie aérienne
      StructField("ORIGIN_AIRPORT_ID", StringType, nullable = true),       // ID de l'aéroport d'origine
      StructField("DEST_AIRPORT_ID", StringType, nullable = true),         // ID de l'aéroport de destination
      StructField("CRS_DEP_TIME", StringType, nullable = true),            // Heure de départ prévue Int
      StructField("ARR_DELAY_NEW", StringType, nullable = true),           // Délai d'arrivée en minutes Double
      StructField("CANCELLED", StringType, nullable = true),               // 1 si annulé, sinon 0 Int
      StructField("DIVERTED", StringType, nullable = true),                // 1 si détourné, sinon 0 int
      StructField("CRS_ELAPSED_TIME", StringType, nullable = true),        // Temps écoulé prévu en minutes Double
      StructField("WEATHER_DELAY", StringType, nullable = true),           // Retard dû au temps en minutes Double
      StructField("NAS_DELAY", StringType, nullable = true),               // Retard dû au système national de l'espace aérien en minutes Double
      StructField("Unnamed: 12", StringType, nullable = true),             // Colonne sans nom (à clarifier)
      StructField("Year", StringType, nullable = true),                    // Année du vol Int
      StructField("FL_DATE", StringType, nullable = true)                  // Date du vol Date
    ))

    //Schéma de la tableau Weather


    //Création des dataframes
    var flight = spark.read.format("csv").option("header", "true").option("delimiter", ",").schema(flightSchema).load(datapath_flight)
    var weather = spark.read.format("csv").option("header", "true").option("delimiter", ",").load(datapath_weather)

    //Transformation de la colonne date du fichier Weather
    weather = weather.withColumn("Date", date_format(to_date(col("Date"), "yyyyMMdd"), "yyyy-MM-dd"))

    //Ajout d'une colonne Year
    flight = flight.withColumn("Year", year(col("FL_DATE")))
    weather = weather.withColumn("Year", year(col("Date")))

    //Création des échantillons
    if (sample) {
      val withReplacement = false //Sans remise
      val fraction = 0.01 //1% de l'échantillon
      val seed = 42 //Seed pour la reproductibilité
      flight = flight.sample(withReplacement, fraction, seed)
      weather = weather.sample(withReplacement, fraction, seed)
    }

    //Ecriture des dataframes en parquet
    flight
      .write
      .mode("overwrite")
      .format("parquet")
      .partitionBy("Year", "FL_DATE")
      .save(outputFile_flight)

    weather
      .write
      .mode("overwrite")
      .format("parquet")
      .partitionBy("Year", "Date")
      .save(outputFile_weather)

  }

}

