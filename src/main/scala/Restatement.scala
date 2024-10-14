import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, DoubleType}
import org.apache.spark.ml.feature.Imputer
import Library._

case object Restatement {

  // Function to create FT Table
  def createFlightTable(df: DataFrame, wban_df: DataFrame): DataFrame = {

    // Handling missing values
    var cleanedDF = df
      .withColumn("CRS_DEP_TIME", lpad(col("CRS_DEP_TIME").cast(IntegerType).cast(StringType), 4, "0")) // Fill with 0 on the left to manage 1 or 2-digit times
      .withColumn("CRS_DEP_TIME", expr("substring(CRS_DEP_TIME, 1, 2) || ':' || substring(CRS_DEP_TIME, 3, 2)"))
      .withColumn("FL_DATE", to_date(col("FL_DATE"), "yyyy-MM-dd"))
      .withColumn("ARR_DELAY_NEW", col("ARR_DELAY_NEW").cast(DoubleType))
      .withColumn("CANCELLED", col("CANCELLED").cast(IntegerType))
      .withColumn("DIVERTED", col("DIVERTED").cast(IntegerType))
      .withColumn("CRS_ELAPSED_TIME", col("CRS_ELAPSED_TIME").cast(DoubleType))
      .withColumn("WEATHER_DELAY", col("WEATHER_DELAY").cast(DoubleType))
      .withColumn("NAS_DELAY", col("NAS_DELAY").cast(DoubleType))

    cleanedDF = cleanedDF
      .drop("Unnamed: 12")
      .na.fill(0, Seq("ARR_DELAY_NEW", "WEATHER_DELAY", "NAS_DELAY"))
      .na.fill("", Seq("CRS_DEP_TIME", "FL_DATE"))

    cleanedDF = cleanedDF
      .withColumn("TIMESTAMP", to_timestamp(concat(col("FL_DATE"), lit(" "), col("CRS_DEP_TIME")), "yyyy-MM-dd HH:mm"))

    // Filtering of diverted and cancelled flights
    val filterDF = cleanedDF
      .where(col("DIVERTED") =!= 1 && col("CANCELLED") =!= 1)
      .drop("DIVERTED", "CANCELLED")

    // Integration of timezone calculations
    // Origin Time Zone
    var newDF = filterDF
      .join(wban_df, filterDF("ORIGIN_AIRPORT_ID") === wban_df("AirportID"), "inner")
      .select(wban_df("TimeZone"), filterDF("*"))
      .withColumnRenamed("TimeZone", "ORIGIN_TIME_ZONE")

    // Destination Time Zone
    newDF = newDF
      .join(wban_df, filterDF("DEST_AIRPORT_ID") === wban_df("AirportID"), "inner")
      .withColumnRenamed("TimeZone", "DEST_TIME_ZONE")

    // Compute Delta Lag
    newDF = newDF
      .withColumn("Delta_Lag", col("DEST_TIME_ZONE").cast(IntegerType) - col("ORIGIN_TIME_ZONE").cast(IntegerType))

    // Final selection of columns with Delta_Lag
    newDF = newDF.select(filterDF("*"), newDF("Delta_Lag"))

    // Add a prefix to each column
    val finalDF = addPrefixToColumns(newDF, "FT_")

    finalDF
  }

  // Function to create OT Table
  def createObservationTable(df_1: DataFrame, wban_df: DataFrame, missingValueRate: Double): DataFrame = {

    // Conversion columns to Double format
    val columnsToConvert = List("StationType","Visibility","DryBulbFarenheit","DryBulbCelsius","WetBulbFarenheit","WetBulbCelsius","DewPointFarenheit","DewPointCelsius","RelativeHumidity", "WindSpeed","WindDirection","ValueForWindCharacter","StationPressure","PressureTendency","PressureChange" ,"SeaLevelPressure", "HourlyPrecip", "Altimeter")
    var newdf = Library.convertColumnsToDouble(df_1, columnsToConvert)

    // Handling missing values in numeric columns
    newdf = imputeMissingValues(newdf, missingValueRate)

    // Columns for values to be corrected (numeric and non-numeric)
    val valueColumns = Seq("SkyCondition", "Visibility", "WeatherType", "DryBulbFarenheit", "DryBulbCelsius", "WetBulbFarenheit", "WetBulbCelsius", "DewPointFarenheit", "DewPointCelsius", "RelativeHumidity", "WindSpeed", "WindDirection", "ValueForWindCharacter", "StationPressure", "PressureTendency", "PressureChange", "SeaLevelPressure", "RecordType", "HourlyPrecip", "Altimeter")
    // Associated flags columns
    val flagColumns = Seq("SkyConditionFlag", "VisibilityFlag", "WeatherTypeFlag", "DryBulbFarenheitFlag", "DryBulbCelsiusFlag", "WetBulbFarenheitFlag", "WetBulbCelsiusFlag", "DewPointFarenheitFlag", "DewPointCelsiusFlag", "RelativeHumidityFlag", "WindSpeedFlag", "WindDirectionFlag", "ValueForWindCharacterFlag", "StationPressureFlag", "PressureTendencyFlag", "PressureChangeFlag", "SeaLevelPressureFlag", "RecordTypeFlag", "HourlyPrecipFlag", "AltimeterFlag")

    newdf = correctValuesBasedOnFlag(newdf, valueColumns, flagColumns)

    // Restatement of Time column to HHmm format with 4 digits
    newdf = newdf.withColumn("Time", lpad(col("Time"), 4, "0"))

    // Restatement of Date column in DateType
    newdf = newdf.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))

    // Creation of TimeStamp column
    newdf = newdf.withColumn("WEATHER_TIMESTAMP", to_timestamp(concat(col("DATE"), lit(" "), col("TIME")), "yyyy-MM-dd HHmm"))

    // Delete of flag columns
    newdf = newdf.drop(flagColumns: _*)

    // Join between WBAN Table and weather table
    // An “inner” join is performed, retaining only common data and excluding all data not linked to an airport.
    // Use the broadcast function to broadcast the smallest table to all cluster nodes and avoid file movements
    newdf = newdf.join(broadcast(wban_df), Seq("WBAN"), "inner")

    // Rename the AIRPORT_ID column
    newdf = newdf.withColumnRenamed("AirportID", "ORIGIN_AIRPORT_ID")

    // Add a prefix to each column
    newdf = addPrefixToColumns(newdf, "OT_")

    newdf
  }

  // Function to impute missing values in a DataFrame
  private def imputeMissingValues(df: DataFrame, missingThreshold: Double = 0.20): DataFrame = {

    // Select numeric columns
    val numericColumns = df.schema.fields.filter(field => field.dataType == "IntegerType" || field.dataType == "DoubleType").map(_.name)

    // Calculate total number of rows
    val totalRows = df.count()

    // Filter columns to impute based on missing values threshold
    val columnsToImpute = numericColumns.filter { colName =>
      val missingCount = df.filter(df(colName).isNull).count()
      val missingPercentage = missingCount.toDouble / totalRows
      missingPercentage < missingThreshold
    }

    // if there are columns to impute
    if (columnsToImpute.nonEmpty) {

      // Initialize the Imputer
      val imputer = new Imputer()
        .setInputCols(columnsToImpute)
        .setOutputCols(columnsToImpute)
        .setStrategy("mean") // ou "median"

      // Apply the imputer
      val model = imputer.fit(df)
      val imputedDF = model.transform(df)

      // Return the imputed DataFrame
      imputedDF

    } else {

      // Return the original DataFrame without modifications
      df

    }
  }

  // Function to correct data flagged with 's' using the mean of the main column
  private def correctValuesBasedOnFlag(df: DataFrame, valueColumns: Seq[String], flagColumns: Seq[String]): DataFrame = {

    var dfCorrected = df
    for ((valueCol, flagCol) <- valueColumns.zip(flagColumns)) {
      // Check if the column is numeric
      val isNumeric = dfCorrected.schema(valueCol).dataType match {
        case _: org.apache.spark.sql.types.NumericType => true
        case _ => false
      }

      // Correction for numeric columns : using the mean
      if (isNumeric) {
        // Calculate the average of values not affected by the “s” flag
        val meanValue = dfCorrected.filter(col(flagCol) =!= "s").agg(avg(col(valueCol))).first().getDouble(0)

        // Replace the values associated with “s” by the average
        dfCorrected = dfCorrected.withColumn(valueCol, when(col(flagCol) === "s", meanValue).otherwise(col(valueCol)))

      } else {
        // Correction for non-numeric columns: use mode (most frequent value)
        val modeValue = dfCorrected.filter(col(flagCol) =!= "s")
          .groupBy(col(valueCol))
          .count()
          .orderBy(desc("count"))
          .first()
          .getString(0)

        // Replace values associated with “s” by the most frequent value (mode)
        dfCorrected = dfCorrected.withColumn(valueCol, when(col(flagCol) === "s", modeValue).otherwise(col(valueCol)))
      }
    }

    dfCorrected
  }

  // Function to generate a dataset for the classification of flights
  def DF_GenerateFlightDataset(in_DF: DataFrame, in_DS_Type: String, in_DelayedThreshold: Double, in_OnTimeThreshold: Double = 0.0): (DataFrame, DataFrame, DataFrame, DataFrame) = {

    // check that the delay threshold is greater than the on-time threshold
    if (in_DelayedThreshold < in_OnTimeThreshold) {
      throw new IllegalArgumentException(s"DF_GenerateDataset: Le seuil de retard $in_DelayedThreshold doit être supérieur au seuil $in_OnTimeThreshold pour les vols à l'heure !")
    }

    // Filtrer les vols annulés ou détournés
    //val df_Filtered = in_DF.filter(in_DF("DIVERTED") =!= 1 && in_DF("CANCELLED") =!= 1)

    //  Create a dataset of delayed flights by DS type
    val out_delayed = in_DS_Type match {
      case "DS1" =>
        in_DF.where(in_DF("FT_ARR_DELAY_NEW") >= in_DelayedThreshold && in_DF("FT_ARR_DELAY_NEW") === (in_DF("FT_WEATHER_DELAY") + in_DF("FT_NAS_DELAY")))
      case "DS2" =>
        in_DF.where(in_DF("FT_ARR_DELAY_NEW") >= in_DelayedThreshold && (in_DF("FT_WEATHER_DELAY") > 0 || in_DF("FT_NAS_DELAY") >= in_DelayedThreshold))
      case "DS3" =>
        in_DF.where(in_DF("FT_ARR_DELAY_NEW") >= in_DelayedThreshold && (in_DF("FT_WEATHER_DELAY") + in_DF("FT_NAS_DELAY") > 0))
      case "DS4" =>
        in_DF.where(in_DF("FT_ARR_DELAY_NEW") >= in_DelayedThreshold)
      case _ =>
        throw new IllegalArgumentException(s"DF_GenerateDataset: Le type de dataset $in_DS_Type n'est pas autorisé (seulement DS1, DS2, DS3, DS4) !")
    }

    // Create a dataset of on-time flights
    val out_OnTime = in_DF.filter(in_DF("FT_ARR_DELAY_NEW") <= in_OnTimeThreshold)

    // Add an 'OnTime' column (False for delayed flights, True for on-time flights)
    val out_delayed_with_OnTime = out_delayed.withColumn("FT_OnTime", lit(0)) // 0 for false
    val out_OnTime_with_OnTime = out_OnTime.withColumn("FT_OnTime", lit(1)) // 1 for true

    //  Counting delayed flights
    val out_delayed_count = out_delayed_with_OnTime.count()

    // Random division of delayed flights into 75% training and 25% test flights
    val Array(out_delayed_train, out_delayed_test) = out_delayed_with_OnTime.randomSplit(Array(0.75, 0.25), seed = 100)

    // Sampling on-time flights to match the number of delayed flights
    val out_OnTime_sampled = out_OnTime_with_OnTime.sample(withReplacement = false, fraction = 1.0).limit(out_delayed_count.toInt)

    // Random division of hourly flights into 75% training and 25% testing
    val Array(out_OnTime_train, out_OnTime_test) = out_OnTime_sampled.randomSplit(Array(0.75, 0.25), seed = 100)

    // Return training and test datasets for delayed and on-time flights
    (out_delayed_train, out_delayed_test, out_OnTime_train, out_OnTime_test)
  }

}