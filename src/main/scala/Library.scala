import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, FloatType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.PrintWriter

object Library {

  // Function for adding prefixes to DataFrame columns
  def addPrefixToColumns(df: DataFrame, prefix: String): DataFrame = {
    df.columns.foldLeft(df)((tempDF, colName) => tempDF.withColumnRenamed(colName, s"$prefix$colName"))
  }

  // Function to rename all columns with a suffix
  def addSuffixToColumns(df: DataFrame, suffix: String): DataFrame = {
    df.columns.foldLeft(df) { (newDF, colName) =>
      newDF.withColumnRenamed(colName, colName + suffix)
    }
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

    // Counter for missing values
    val missing_values = df.schema.fields.map { field =>
      val colName = field.name
      val missingCount = field.dataType match {
        case DoubleType | FloatType =>
          df.filter(df(colName).isNull || df(colName).isNaN).count()

        case StringType =>
          df.filter(df(colName).isNull || df(colName) === "").count()

        case _ =>
          df.filter(df(colName).isNull).count()
      }
      Row(colName, missingCount)
    }

    // Define the schema for the resulting DataFrame
    val schema = StructType(List(
      StructField("Column", StringType, nullable = false),
      StructField("Missing_values", LongType, nullable = false)
    ))

    // Create DataFrame from the missing values
    val missing_values_df = spark.createDataFrame(spark.sparkContext.parallelize(missing_values), schema)

    // Calculate the percentage of missing values
    val totalRows = df.count()
    val missing_values_with_percentage = missing_values_df.withColumn("Pct_missing_values", col("Missing_values") / totalRows * 100)

    missing_values_with_percentage
  }

  // Function to export the schema of a DataFrame
  def exportSchema(df: DataFrame, outputPath: String): Unit = {

    // Retrieve the schema in JSON format
    val schemaJson = df.schema.json

    // Write the schema to a file
    new PrintWriter(outputPath) {
      write(schemaJson)
      close()
    }
  }

}
