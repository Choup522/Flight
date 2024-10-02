import org.apache.spark.sql.types.{IntegerType,StringType,StructType,StructField, DoubleType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.util.Date

object Statistiques {

  // Function to count the number of flights per year
  def countAirport(flight: DataFrame): DataFrame = {
    val desc = flight
      .groupBy("ORIGIN_AIRPORT_ID")
      .agg(
        count(col("OP_CARRIER_FL_NUM")).alias("nbs de vols"),
        countDistinct(col("OP_CARRIER_AIRLINE_ID")).alias("nbs de compagnies"),
        countDistinct(col("DEST_AIRPORT_ID")).alias("nbs d'aeroports - arrivée")
      )

    desc
  }


  def countCarrier(flight: DataFrame,onTimeParam: Int ): DataFrame = {

    val flightPerf = flight
      .withColumn("DELAYED", when(col("ARR_DELAY_NEW") >= onTimeParam, lit(1)).otherwise(lit(0)))
      .withColumn("ONTIME", when(col("ARR_DELAY_NEW") <= onTimeParam, lit(1)).otherwise(lit(0)))
      .groupBy("year")
      .agg(
        count(col("OP_CARRIER_FL_NUM")).alias("nbs de vol"),
        count(when(col("ONTIME") === 1, 1)).alias("On-Time"),
        count(when(col("DELAYED") === 1, 1)).alias("Delayed"),
        count(when(col("CANCELLED") === 1, 1)).alias("Cancelled"),
        count(when(col("DIVERTED") === 1, 1)).alias("Diverted")
      )
      .withColumn("TOTAL", col("On-Time") + col("Delayed") + col("Cancelled") + col("Diverted")) // A RENOMMER
      .withColumn("% On-time", col("On-Time") / col("TOTAL") * 100)
      .withColumn("% Delayed", col("Delayed") / col("TOTAL") * 100)
      .withColumn("% Cancelled", col("Cancelled") / col("TOTAL") * 100)
      .withColumn("% Diverted", col("Diverted") / col("TOTAL") * 100)
      .drop("On-Time", "Delayed", "Cancelled","Diverted", "TOTAL")

    flightPerf

  }

  // Function to count the number of flights per year
  def countDelayedFlight(flight: DataFrame): DataFrame = {

    val delayedData = flight
      .where(col("ARR_DELAY_NEW") =!= 0)
      .groupBy("year")
      .agg(
        sum(col("ARR_DELAY_NEW")).alias("ARR_DELAY_NEW"),
        sum(col("WEATHER_DELAY")).alias("WEATHER_DELAY"),
        sum(col("NAS_DELAY")).alias("NAS_DELAY"))
      .withColumn("TOTAL", col("NAS_DELAY") + col("WEATHER_DELAY"))
      .withColumn("OTHER_DELAY", col("ARR_DELAY_NEW") - col("TOTAL"))
      .withColumn("% Weather_delay", col("WEATHER_DELAY") / col("ARR_DELAY_NEW") * 100)
      .withColumn("% Nas_delay", col("NAS_DELAY") / col("ARR_DELAY_NEW") * 100)
      .withColumn("% Other_delay", col("OTHER_DELAY") / col("ARR_DELAY_NEW") * 100)
      .drop("WEATHER_DELAY", "NAS_DELAY","TOTAL", "ARR_DELAY_NEW", "OTHER_DELAY")

    delayedData
  }

  def countFlagsWithSAndExport(df: DataFrame): DataFrame = {

    // Liste des colonnes se terminant par 'Flag'
    val flagColumns = df.columns.filter(_.endsWith("Flag"))

    // Compteur des occurences dans chaque colonne
    val flagCounts = flagColumns.map { colName =>
      val count = df.filter(col(colName) === "s").count()
      (colName, count)
    }

    // Conversion en dataframe
    val countDf = df.sparkSession.createDataFrame(flagCounts).toDF("FlagColumn", "Count")

    countDf

  }

}
