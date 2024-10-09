import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

case object JoinOperations {

  // Function to prepare the DataFrame for the join operation
  def DF_Map(in_DF: DataFrame, in_DF_Type: String): DataFrame = {

    // Adding a column to identify the type of DataFrame
    var out_Map = in_DF.withColumn("TAG", lit(in_DF_Type))

    // Operation for the weather observation table (OT)
    if (in_DF_Type == "OT") {

      // Creation of the join key JOIN_KEY by concatenating ORIGIN_AIRPORT_ID and Date
      out_Map = out_Map.withColumn("OT_JOIN_KEY", concat(col("OT_ORIGIN_AIRPORT_ID"), lit("_"), col("OT_Date")))

    } else if (in_DF_Type == "FT") {

      // Creation of the column DEST_DATE_TIME representing the time of arrival at the destination airport and conversion in seconds
      out_Map = out_Map.withColumn("FT_DEST_DATE_TIME", from_unixtime(unix_timestamp(col("FT_TIMESTAMP")) + (col("FT_CRS_ELAPSED_TIME") * 60).cast("long") + (col("FT_Delta_Lag") * 3600).cast("long")).cast(TimestampType))

      // Creation of the column ORIGIN_DATE_TIME representing the time of departure from the origin airport and conversion in seconds
      //out_Map = out_Map.withColumn("FT_DEST_DATE_TIME", (col("FT_TIMESTAMP") + col("FT_CRS_ELAPSED_TIME") * 60 + col("FT_Delta_Lag") * 3600).cast(TimestampType))

      // Creation of the join key for the origin airport JOIN_KEY_ORIGIN
      out_Map = out_Map.withColumn("FT_JOIN_KEY_ORIGIN", concat(col("FT_ORIGIN_AIRPORT_ID"), lit("_"), col("FT_FL_DATE")))

      // Creation of the join key for the destination airport JOIN_KEY_DEST
      out_Map = out_Map.withColumn("FT_JOIN_KEY_DEST", concat(col("FT_DEST_AIRPORT_ID"), lit("_"), col("FT_DEST_DATE_TIME")))

    } else {
      throw new IllegalArgumentException(s"DF_Map: error $in_DF_Type dataframe type not allowed (OT,FT) !")
    }

    out_Map
  }

  // Function to reduce the DataFrame
  def DF_Reduce(df_flights_parquet: DataFrame, df_weather_parquet: DataFrame) : DataFrame = {

    // Sort datetime in ascending order
    val df_weather_sorted = df_weather_parquet.sort(asc("OT_WEATHER_TIMESTAMP"))
    val df_flights_sorted = df_flights_parquet.sort(asc("FT_TIMESTAMP"))

    // Will loop on hour to generate and retrieve corresponding weather observations
    val hours_lag = 0 to 11
    var df_flights = df_flights_sorted

    for (h <- hours_lag) {
      // Generate new columns indicating exact datetime of lagged Flight datetime
      df_flights = df_flights
        .withColumn(s"FT_ORIGIN_DATE_TIME_PART_$h", col("FT_TIMESTAMP") - expr(s"INTERVAL $h HOURS"))
        .withColumn(s"FT_DEST_DATE_TIME_PART_$h", col("FT_DEST_DATE_TIME") - expr(s"INTERVAL $h HOURS"))
    }

    // Join the flights with the weather data
    var df_result = df_flights
    for (h <- hours_lag) {
      // Create temporary weather dataframe for origin join
      val df_weather_origin = df_weather_sorted
        .withColumn(s"OT_DATE_TIME_Part_$h", col("OT_WEATHER_TIMESTAMP"))
        //.withColumnRenamed("OT_WEATHER_TIMESTAMP", s"OT_DATE_TIME_Part_$h")
        .select(col(s"OT_DATE_TIME_Part_$h") +: df_weather_sorted.columns.map(c => col(c).alias(s"${c}_Part_$h")): _*)

      // Create temporary weather dataframe for destination join
      val df_weather_dest = df_weather_sorted
        .withColumn(s"OT_DEST_DATE_TIME_Part_$h", col("OT_WEATHER_TIMESTAMP"))
        //.withColumnRenamed("OT_WEATHER_TIMESTAMP", s"OT_DEST_DATE_TIME_Part_$h")
        .select(col(s"OT_DEST_DATE_TIME_Part_$h") +: df_weather_sorted.columns.map(c => col(c).alias(s"${c}_Dest_Part_$h")): _*)

      // Join flights with weather data on origin date
      df_result = df_result
        .join(df_weather_origin, col(s"FT_ORIGIN_DATE_TIME_PART_$h") === col(s"OT_DATE_TIME_Part_$h"), "left")
      // Join flights with weather data on destination date
        .join(df_weather_dest, col(s"FT_DEST_DATE_TIME_PART_$h") === col(s"OT_DEST_DATE_TIME_Part_$h"), "left")
    }

    df_result
  }

}

