import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object Library {

  // Function for adding prefixes to DataFrame columns
  def addPrefixToColumns(df: DataFrame, prefix: String): DataFrame = {
    df.columns.foldLeft(df)((tempDF, colName) => tempDF.withColumnRenamed(colName, s"$prefix$colName"))
  }

  // Function to convert columns to Double
  def convertColumnsToDouble(df: DataFrame, columns: List[String]): DataFrame = {
    columns.foldLeft(df) { (tempDf, colName) => tempDf.withColumn(colName, col(colName).cast("Double")) }
  }

  // Function for exporting data in csv format
  def exportDataToCSV(df: DataFrame, outputPath: String): Unit = {
    df
      .coalesce(1) // Save as a single file
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(outputPath)
  }

  // Function to create a DataFrame from a sequence of tuples
  def createExecutionTimesDataFrame(spark: SparkSession, times: Seq[(String, Double)]): DataFrame = {
    import spark.implicits._
    times.toDF("Task", "Duration (seconds)")
  }

  // Function to count missing values
  def count_missing_values(df: DataFrame, spark: SparkSession): DataFrame = {

    // counter for missing values
    val missing_values = df.columns.map { colName =>
      val missingCount = df.filter(col(colName).isNull).count()
      Row(colName, missingCount)
    }

    // Define the schema
    val schema = StructType(List(
      StructField("Column", StringType, nullable = false),
      StructField("Missing_values", LongType, nullable = false)
    ))

    // Creation of the DataFrame from the missing values
    val missing_values_df = spark.createDataFrame(spark.sparkContext.parallelize(missing_values), schema)

    // Calculate the percentage of missing values
    val totalRows = df.count()
    val missing_values_with_percentage = missing_values_df.withColumn("Pct_missing_values", col("Missing_values") / totalRows * 100)

    missing_values_with_percentage
  }

}
