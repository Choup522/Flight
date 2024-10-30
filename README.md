# Flight Classification with Random Forest

## Description

This project is a Scala application that uses Apache Spark to classify flight data. The application processes flight data, imputes missing values, corrects flagged data, and generates datasets for training and testing a Random Forest classifier.

## Features

- **Data Preprocessing**: 
  - Impute missing values in numeric columns.
  - Correct values based on flags.
  - Generate datasets for delayed and on-time flights.
- **Random Forest Classification**:
  - Convert string and date columns to numerical values.
  - Train a Random Forest model using cross-validation.
  - Evaluate the model and save metrics.

## Requirements

- Scala
- sbt (Simple Build Tool)
- Apache Spark
- IntelliJ IDEA (recommended for development)

## Installation

1. **Clone the repository**:
    ```sh
    git clone https://github.com/Choup522/Flight.git
    cd Flight
    ```

2. **Set up the project**:
    - Open the project in IntelliJ IDEA.
    - Ensure that the Scala and sbt plugins are installed.

3. **Install dependencies**:
    ```sh
    sbt update
    ```

## Usage

### Data Preprocessing

The `Restatement.scala` file contains functions for data preprocessing:

- **Impute Missing Values**:
    ```scala
    val imputedDF = imputeMissingValues(df)
    ```

- **Correct Values Based on Flags**:
    ```scala
    val correctedDF = correctValuesBasedOnFlag(df, valueColumns, flagColumns)
    ```

- **Generate Flight Dataset**:
    ```scala
    val (delayedTrain, delayedTest, onTimeTrain, onTimeTest) = DF_GenerateFlightDataset(df, "DS1", 15.0)
    ```

### Random Forest Classification

The `RandomForest.scala` file contains functions for training and evaluating a Random Forest model:

- **Train and Evaluate Model**:
    ```scala
    val metrics = RandomForest.randomForest(df, "labelColumn", Array("feature1", "feature2"))
    ```

- **Save Metrics**:
    ```scala
    RandomForest.saveMetricsAsCSV(spark, metrics, "output/path")
    ```

## Example

Here is an example of how to use the application:

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.appName("Flight Classification").getOrCreate()
val df = spark.read.option("header", "true").csv("path/to/flight_data.csv")

// Data Preprocessing
val imputedDF = imputeMissingValues(df)
val correctedDF = correctValuesBasedOnFlag(imputedDF, Seq("valueColumn1", "valueColumn2"), Seq("flagColumn1", "flagColumn2"))
val (delayedTrain, delayedTest, onTimeTrain, onTimeTest) = DF_GenerateFlightDataset(correctedDF, "DS1", 15.0)

// Random Forest Classification
val metrics = RandomForest.randomForest(correctedDF, "labelColumn", Array("feature1", "feature2"))
RandomForest.saveMetricsAsCSV(spark, metrics, "output/path")
