import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Parquet {

  def createParquetFile(datapath_flight: String, datapath_weather: String, outputFile_flight: String, outputFile_weather: String, spark: SparkSession, sample: Boolean): Unit = {

    // Création des dataframes
    var flight = spark.read.format("csv").option("header", "true").option("delimiter", ",").load(datapath_flight)
    var weather = spark.read.format("csv").option("header", "true").option("delimiter", ",").load(datapath_weather)

    // Transforming the date column in the Weather file
    weather = weather.withColumn("Date", date_format(to_date(col("Date"), "yyyyMMdd"), "yyyy-MM-dd"))

    // ARR_DELAY_NEW column transformation
    flight = flight.withColumn("ARR_DELAY_NEW", col("ARR_DELAY_NEW").cast("double"))

    // Add Year column
    flight = flight.withColumn("Year", year(col("FL_DATE")))
    weather = weather.withColumn("Year", year(col("Date")))

    // Sample creation
    if (sample) {
      val withReplacement = false // Without replacement
      val fraction = 0.01 // 1% of the data
      val seed = 42 // Seed for reproducibility
      flight = flight.sample(withReplacement, fraction, seed)
      weather = weather.sample(withReplacement, fraction, seed)
    }

    // Write the dataframes in parquet format
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

  def readParquetFiles(flightOutputFile: String, weatherOutputFile: String, spark: SparkSession): (DataFrame, DataFrame)  = {

    // Schema of the Flight table
    val flightSchema = StructType(Array(
      StructField("FL_DATE", DateType, nullable = true),                   // Date du vol Date
      StructField("OP_CARRIER_AIRLINE_ID", StringType, nullable = true),   // ID de la compagnie aérienne
      StructField("OP_CARRIER_FL_NUM", StringType, nullable = true),       // Numéro de vol de la compagnie aérienne
      StructField("ORIGIN_AIRPORT_ID", StringType, nullable = true),       // ID de l'aéroport d'origine
      StructField("DEST_AIRPORT_ID", StringType, nullable = true),         // ID de l'aéroport de destination
      StructField("CRS_DEP_TIME", IntegerType, nullable = true),           // Heure de départ prévue Int
      StructField("ARR_DELAY_NEW", DoubleType, nullable = true),           // Délai d'arrivée en minutes Double
      StructField("CANCELLED", IntegerType, nullable = true),              // 1 si annulé, sinon 0 Int
      StructField("DIVERTED", IntegerType, nullable = true),               // 1 si détourné, sinon 0 int
      StructField("CRS_ELAPSED_TIME", DoubleType, nullable = true),        // Temps écoulé prévu en minutes Double
      StructField("WEATHER_DELAY", DoubleType, nullable = true),           // Retard dû au temps en minutes Double
      StructField("NAS_DELAY", DoubleType, nullable = true),               // Retard dû au système national de l'espace aérien en minutes Double
      StructField("Unnamed: 12", StringType, nullable = true),             // Colonne sans nom (à clarifier)
      StructField("Year", StringType, nullable = true)                     // Année du vol In
    ))

    // Schema of the Weather table
    val weatherSchema = StructType(Array(
      StructField("_c0", StringType, nullable = true), // Index or internal ID (usually not relevant)
      StructField("WBAN", StringType, nullable = true), // Weather Bureau Army Navy identifier for the station
      StructField("Time", StringType, nullable = true), // Time of observation (usually in UTC or local time)
      StructField("StationType", StringType, nullable = true), // Type of weather station (e.g., Automated or Manual)
      StructField("SkyCondition", StringType, nullable = true), // Description of sky conditions (e.g., clear, cloudy)
      StructField("SkyConditionFlag", StringType, nullable = true), // Flag for SkyCondition (indicating data quality)
      StructField("Visibility", DoubleType, nullable = true), // Visibility in miles or kilometers
      StructField("VisibilityFlag", StringType, nullable = true), // Flag for Visibility (indicating data quality)
      StructField("WeatherType", StringType, nullable = true), // Type of weather observed (e.g., rain, snow)
      StructField("WeatherTypeFlag", StringType, nullable = true), // Flag for WeatherType (indicating data quality)
      StructField("DryBulbFarenheit", DoubleType, nullable = true), // Dry bulb temperature in Fahrenheit
      StructField("DryBulbFarenheitFlag", StringType, nullable = true), // Flag for DryBulbFarenheit (indicating data quality)
      StructField("DryBulbCelsius", DoubleType, nullable = true), // Dry bulb temperature in Celsius
      StructField("DryBulbCelsiusFlag", StringType, nullable = true), // Flag for DryBulbCelsius (indicating data quality)
      StructField("WetBulbFarenheit", DoubleType, nullable = true), // Wet bulb temperature in Fahrenheit
      StructField("WetBulbFarenheitFlag", StringType, nullable = true), // Flag for WetBulbFarenheit (indicating data quality)
      StructField("WetBulbCelsius", DoubleType, nullable = true), // Wet bulb temperature in Celsius
      StructField("WetBulbCelsiusFlag", StringType, nullable = true), // Flag for WetBulbCelsius (indicating data quality)
      StructField("DewPointFarenheit", DoubleType, nullable = true), // Dew point temperature in Fahrenheit
      StructField("DewPointFarenheitFlag", StringType, nullable = true), // Flag for DewPointFarenheit (indicating data quality)
      StructField("DewPointCelsius", DoubleType, nullable = true), // Dew point temperature in Celsius
      StructField("DewPointCelsiusFlag", StringType, nullable = true), // Flag for DewPointCelsius (indicating data quality)
      StructField("RelativeHumidity", DoubleType, nullable = true), // Relative humidity percentage
      StructField("RelativeHumidityFlag", StringType, nullable = true), // Flag for RelativeHumidity (indicating data quality)
      StructField("WindSpeed", DoubleType, nullable = true), // Wind speed in miles per hour or kilometers per hour
      StructField("WindSpeedFlag", StringType, nullable = true), // Flag for WindSpeed (indicating data quality)
      StructField("WindDirection", DoubleType, nullable = true), // Wind direction in degrees (0-360)
      StructField("WindDirectionFlag", StringType, nullable = true), // Flag for WindDirection (indicating data quality)
      StructField("ValueForWindCharacter", StringType, nullable = true), // Character value for wind description
      StructField("ValueForWindCharacterFlag", StringType, nullable = true), // Flag for ValueForWindCharacter (indicating data quality)
      StructField("StationPressure", DoubleType, nullable = true), // Atmospheric pressure at station level
      StructField("StationPressureFlag", StringType, nullable = true), // Flag for StationPressure (indicating data quality)
      StructField("PressureTendency", StringType, nullable = true), // Trend of pressure change (e.g., rising, falling)
      StructField("PressureTendencyFlag", StringType, nullable = true), // Flag for PressureTendency (indicating data quality)
      StructField("PressureChange", StringType, nullable = true), // Amount of pressure change over time
      StructField("PressureChangeFlag", StringType, nullable = true), // Flag for PressureChange (indicating data quality)
      StructField("SeaLevelPressure", DoubleType, nullable = true), // Atmospheric pressure at sea level
      StructField("SeaLevelPressureFlag", StringType, nullable = true), // Flag for SeaLevelPressure (indicating data quality)
      StructField("RecordType", StringType, nullable = true), // Type of record (e.g., observation, forecast)
      StructField("RecordTypeFlag", StringType, nullable = true), // Flag for RecordType (indicating data quality)
      StructField("HourlyPrecip", DoubleType, nullable = true), // Precipitation measured over the hour
      StructField("HourlyPrecipFlag", StringType, nullable = true), // Flag for HourlyPrecip (indicating data quality)
      StructField("Altimeter", StringType, nullable = true), // Altimeter setting (used for aviation)
      StructField("AltimeterFlag", StringType, nullable = true), // Flag for Altimeter (indicating data quality)
      StructField("Year", IntegerType, nullable = true), // Year of observation
      StructField("Date", DateType, nullable = true) // Date of observation
    ))

    val flight = spark.read.parquet(flightOutputFile)
    val weather = spark.read.parquet(weatherOutputFile)

    (flight, weather)
  }

}

