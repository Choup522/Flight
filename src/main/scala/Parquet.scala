import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger

object Parquet {

  // Initialize the logger
  private val logger = Logger.getLogger("Parquet_Logger")

  def createParquetFile(datapath_flight: String, datapath_weather: String, outputFile_flight: String, outputFile_weather: String, spark: SparkSession, sample: Boolean): Unit = {

    logger.info("createParquetFile: Starting Parquet file creation")

    // Création des dataframes
    logger.info("createParquetFile: Reading CSV files")
    var flight = spark.read.format("csv").option("header", "true").option("delimiter", ",").load(datapath_flight)
    logger.info("createParquetFile: Flight CSV file read")
    var weather = spark.read.format("csv").option("header", "true").option("delimiter", ",").load(datapath_weather)

    // Transforming the date column in the Weather file
    logger.info("createParquetFile: Transforming the date column in the Weather file")
    weather = weather.withColumn("Date", date_format(to_date(col("Date"), "yyyyMMdd"), "yyyy-MM-dd"))

    // ARR_DELAY_NEW column transformation
    logger.info("createParquetFile: ARR_DELAY_NEW column transformation")
    flight = flight.withColumn("ARR_DELAY_NEW", col("ARR_DELAY_NEW").cast("double"))

    // Add Year column
    logger.info("createParquetFile: Adding Year column")
    flight = flight.withColumn("Year", year(col("FL_DATE")))
    weather = weather.withColumn("Year", year(col("Date")))

    // Sample creation
    if (sample) {
      logger.info("createParquetFile: Sample creation")
      val withReplacement = false // Without replacement
      val fraction = 0.01 // 1% of the data
      val seed = 42 // Seed for reproducibility
      flight = flight.sample(withReplacement, fraction, seed)
      weather = weather.sample(withReplacement, fraction, seed)
    }

    // Write the dataframes in parquet format
    logger.info("createParquetFile: Writing the dataframes in parquet format")
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

    logger.info("createParquetFile: Parquet file creation completed")
  }

  def readParquetFiles(OutputFile_1: String, OutputFile_2: String, spark: SparkSession, Statut: Boolean): (DataFrame, DataFrame)  = {

    // Reading the parquet files
    logger.info("readParquetFiles: Reading the parquet files")
    val flight = spark.read.parquet(OutputFile_1)
    val weather = spark.read.parquet(OutputFile_2)

    if (Statut) {
      // Find common date ranges
      logger.info("readParquetFiles: Finding common date ranges")
      val flightDateRange = flight.agg(min("FL_DATE").alias("min_date"), max("FL_DATE").alias("max_date")).collect()(0)
      val weatherDateRange = weather.agg(min("Date").alias("min_date"), max("Date").alias("max_date")).collect()(0)

      // Check if the date ranges overlap
      logger.info("readParquetFiles: Checking if the date ranges overlap")
      val commonStartDate = if (flightDateRange.getAs[java.sql.Date]("min_date").after(weatherDateRange.getAs[java.sql.Date]("min_date")))
        flightDateRange.getAs[java.sql.Date]("min_date")
      else
        weatherDateRange.getAs[java.sql.Date]("min_date")

      val commonEndDate = if (flightDateRange.getAs[java.sql.Date]("max_date").before(weatherDateRange.getAs[java.sql.Date]("max_date")))
        flightDateRange.getAs[java.sql.Date]("max_date")
      else
        weatherDateRange.getAs[java.sql.Date]("max_date")

      // Filter dataframes on common date range
      logger.info("readParquetFiles: Filtering dataframes on common date range")
      val filteredFlight = flight.filter(flight("FL_DATE").between(commonStartDate, commonEndDate))
      val filteredWeather = weather.filter(weather("Date").between(commonStartDate, commonEndDate))

      (filteredFlight, filteredWeather)
    } else {
        (flight, weather)
    }
  }

  // Function to store the dataframes in parquet format
  def storeParquetFiles(df: DataFrame, outputPath: String): Unit = {

    logger.info("storeParquetFiles: Storing the dataframes in parquet format")

    df
      .write
      .mode("overwrite")
      .format("parquet")
      .save(outputPath)

    logger.info("storeParquetFiles: Dataframes stored in parquet format")
  }

}

