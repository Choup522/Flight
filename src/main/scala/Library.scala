import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


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

  // Function to collect the information on the cluster and partitions
  def getClusterInfo(df: DataFrame, sc: SparkContext, spark: SparkSession): (Int, Int, Int, Int) = {

    // Collect information on the cluster
    val executorInfo = sc.statusTracker.getExecutorInfos

    // Calculate the total number of cores using the information from the executors
    val totalCores = executorInfo.map(info => info.numRunningTasks).sum

    // Collect the number of partitions used by the DataFrame
    val numPartitions = df.rdd.getNumPartitions

    // Collect the size of the data in the DataFrame
    val totalSizeInBytes = df.rdd
      .map(_.toString.getBytes("UTF-8").length.toLong)
      .reduce(_ + _)

    // Convert the size to megabytes
    val totalSizeInMB = totalSizeInBytes / (1024 * 1024)

    // Calculate the theoretical partitions based on the number of cores and the size of the data
    val partitionsBasedOnCoresMin: Int = math.max(totalCores * 2, 1)
    val partitionsBasedOnCoresMax: Int = math.max(totalCores * 4, 1)

    // Calculate the theoretical partitions based on the size of the data (128 MB to 256 MB per partition)
    val partitionsBasedOnSizeMin = math.ceil(totalSizeInMB / 256).toInt
    val partitionsBasedOnSizeMax = math.ceil(totalSizeInMB / 128).toInt

    // Return the information
    (partitionsBasedOnCoresMin, partitionsBasedOnCoresMax, partitionsBasedOnSizeMin, partitionsBasedOnSizeMax)
  }

}
