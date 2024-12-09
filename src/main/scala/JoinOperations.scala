import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.log4j.Logger
import Library._
import LoggerFactory.logger

case object JoinOperations {

  // Function to prepare the DataFrame for the join operation
  def DF_Map(in_DF: DataFrame, in_DF_Type: String): DataFrame = {

    logger.info(s"DF_Map: Processing DataFrame of type $in_DF_Type")

    // Adding a column to identify the type of DataFrame
    var out_Map = in_DF.withColumn("TAG", lit(in_DF_Type))
    logger.info(s"DF_Map: Added TAG column with value $in_DF_Type")

    // Operation for the weather observation table (OT)
    if (in_DF_Type == "OT") {

      logger.info(s"DF_Map: Processing OT DataFrame")

      // Creation of the join key JOIN_KEY by concatenating ORIGIN_AIRPORT_ID and Date
      out_Map = out_Map.withColumn("OT_JOIN_KEY", concat(col("OT_ORIGIN_AIRPORT_ID"), lit("_"), col("OT_Date")))
      logger.info(s"DF_Map: Created OT_JOIN_KEY column")

    } else if (in_DF_Type == "FT") {

      logger.info(s"DF_Map: Processing FT DataFrame")

      // Creation of the column DEST_DATE_TIME representing the time of arrival at the destination airport and conversion in seconds
      out_Map = out_Map.withColumn("FT_DEST_DATE_TIME", from_unixtime(unix_timestamp(col("FT_TIMESTAMP")) + (col("FT_CRS_ELAPSED_TIME") * 60).cast("long") + (col("FT_Delta_Lag") * 3600).cast("long")).cast(TimestampType))
      logger.info(s"DF_Map: Created FT_DEST_DATE_TIME column")

      // Creation of the column ORIGIN_DATE_TIME representing the time of departure from the origin airport and conversion in seconds
      //out_Map = out_Map.withColumn("FT_DEST_DATE_TIME", (col("FT_TIMESTAMP") + col("FT_CRS_ELAPSED_TIME") * 60 + col("FT_Delta_Lag") * 3600).cast(TimestampType))

      // Creation of the join key for the origin airport JOIN_KEY_ORIGIN
      out_Map = out_Map.withColumn("FT_JOIN_KEY_ORIGIN", concat(col("FT_ORIGIN_AIRPORT_ID"), lit("_"), col("FT_FL_DATE")))
      logger.info(s"DF_Map: Created FT_JOIN_KEY_ORIGIN column")

      // Creation of the join key for the destination airport JOIN_KEY_DEST
      out_Map = out_Map.withColumn("FT_JOIN_KEY_DEST", concat(col("FT_DEST_AIRPORT_ID"), lit("_"), col("FT_DEST_DATE_TIME")))
      logger.info(s"DF_Map: Created FT_JOIN_KEY_DEST column")

    } else {
      logger.error(s"DF_Map: error $in_DF_Type dataframe type not allowed (OT,FT)!")
      throw new IllegalArgumentException(s"DF_Map: error $in_DF_Type dataframe type not allowed (OT,FT) !")
    }

    logger.info(s"DF_Map: Completed processing for DataFrame of type $in_DF_Type")
    out_Map
  }

  // Function to join dataframes based on columns
  def DF_Reduce_Cols(FT_flights: DataFrame, OT_weather: DataFrame, numPartitions: Int = 100) : DataFrame = {

    // Sort datetime in ascending order
    logger.info("DF_Reduce Cols: Starting DataFrame reduction")
    val df_weather_sorted = OT_weather.sort(asc("OT_WEATHER_TIMESTAMP")).repartitionByRange(numPartitions, col("OT_WEATHER_TIMESTAMP"))
    val df_flights_sorted = FT_flights.sort(asc("FT_TIMESTAMP")).repartitionByRange(numPartitions, col("FT_TIMESTAMP"))
    logger.info("DF_Reduce Cols: Sorted weather and flight data by OT_WEATHER_TIMESTAMP")

    // Will loop on hour to generate and retrieve corresponding weather observations
    val hours_lag = 0 to 11

    // Generate columns for lag hours
    logger.info("DF_Reduce Cols: Creating columns for lag hours")
    val df_flights = df_flights_sorted.select(
      col("*") +: hours_lag.flatMap(h => Seq(
        (col("FT_TIMESTAMP") - expr(s"INTERVAL $h HOURS")).as(s"FT_ORIGIN_DATE_TIME_PART_$h"),
        (col("FT_DEST_DATE_TIME") - expr(s"INTERVAL $h HOURS")).as(s"FT_DEST_DATE_TIME_PART_$h")
      )): _*
    )

    // Join the flights with the weather data
    var df_result = df_flights
    for (h <- hours_lag) {
      logger.info(s"DF_Reduce Cols: Performing join for lag hour $h")
      // Create temporary weather dataframe for origin join
      val df_weather_origin = df_weather_sorted
        .withColumn(s"OT_DATE_TIME_Part_$h", col("OT_WEATHER_TIMESTAMP"))
        .select(col(s"OT_DATE_TIME_Part_$h") +: df_weather_sorted.columns.map(c => col(c).alias(s"${c}_Part_$h")): _*)

      // Create temporary weather dataframe for destination join
      val df_weather_dest = df_weather_sorted
        .withColumn(s"OT_DEST_DATE_TIME_Part_$h", col("OT_WEATHER_TIMESTAMP"))
        .select(col(s"OT_DEST_DATE_TIME_Part_$h") +: df_weather_sorted.columns.map(c => col(c).alias(s"${c}_Dest_Part_$h")): _*)
      logger.info(s"DF_Reduce Cols: Prepared weather data for destination join for lag hour $h")

      // Join flights with weather data on origin date
      df_result = df_result
        .join(df_weather_origin, col(s"FT_ORIGIN_DATE_TIME_PART_$h") === col(s"OT_DATE_TIME_Part_$h"), "left")
        .join(df_weather_dest, col(s"FT_DEST_DATE_TIME_PART_$h") === col(s"OT_DEST_DATE_TIME_Part_$h"), "left")
      logger.info(s"DF_Reduce Cols: Completed join for lag hour $h")
    }

    logger.info("DF_Reduce Cols: Completed DataFrame reduction")
    df_result
  }

  // Function to join dataframes based on rows
  def DF_Reduce_Line(FT_flights: DataFrame, OT_weather: DataFrame, spark: SparkSession): DataFrame = {

    logger.info("DF_Reduce_Line: Starting DataFrame reduction with rows per hour")

    import spark.implicits._

    // Step 1: Create a dataset of hours_lag from 0 to 11
    val hours_lag = spark.createDataset(0 to 11).toDF("hours_lag")

    // Step 2: Select relevant columns and add hours_lag to the flights dataframe
    val df_flights_with_lag = FT_flights
      //.select("FT_ORIGIN_AIRPORT_ID", "FT_DEST_AIRPORT_ID", "FT_TIMESTAMP", "FT_DEST_DATE_TIME")
      .withColumn("FT_ORIGIN_UNIX_TS", unix_timestamp($"FT_TIMESTAMP"))
      .withColumn("FT_DEST_UNIX_TS", unix_timestamp($"FT_DEST_DATE_TIME"))
      .crossJoin(hours_lag)
      .withColumn("FT_ORIGIN_DATE_TIME_LAG", expr("from_unixtime(FT_ORIGIN_UNIX_TS - hours_lag * 3600)"))
      .withColumn("FT_DEST_DATE_TIME_LAG", expr("from_unixtime(FT_DEST_UNIX_TS - hours_lag * 3600)"))
      .repartition($"FT_ORIGIN_AIRPORT_ID", $"FT_DEST_AIRPORT_ID")  // Repartition by airport ID for optimization
      .cache()

    logger.info("DF_Reduce_Line: Created lagged timestamps for each flight")

    // Step 3: Rename all columns for the weather dataframe to avoid ambiguity in the join for origin and destination
    val df_weather_origin = addSuffixToColumns(OT_weather, "_ORIG")
    val df_weather_dest = addSuffixToColumns(OT_weather, "_DEST")

    // Step 4: Repartition weather data by airport for more efficient joins
    val df_weather_origin_partitioned = df_weather_origin.repartition($"OT_ORIGIN_AIRPORT_ID_ORIG")
    val df_weather_dest_partitioned = df_weather_dest.repartition($"OT_ORIGIN_AIRPORT_ID_DEST")

    // Step 5: Join the flights with the weather data for the origin and destination airports
    val df_result = df_flights_with_lag
      .join(broadcast(df_weather_origin_partitioned), col("FT_ORIGIN_DATE_TIME_LAG") === col("OT_WEATHER_TIMESTAMP_ORIG") && col("FT_ORIGIN_AIRPORT_ID") === col("OT_ORIGIN_AIRPORT_ID_ORIG"), "left")
      .join(broadcast(df_weather_dest_partitioned), col("FT_DEST_DATE_TIME_LAG") === col("OT_WEATHER_TIMESTAMP_DEST") && col("FT_DEST_AIRPORT_ID") === col("OT_ORIGIN_AIRPORT_ID_DEST"), "left")

    logger.info("DF_Reduce_Line: Completed DataFrame reduction with rows per hour")

    // Step 6: Return the final DataFrame
    df_result
  }

}


