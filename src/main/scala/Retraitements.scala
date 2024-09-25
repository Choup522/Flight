import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, DoubleType}

case object Retraitements {

  // Fonction pour détecter et compter les valeurs manquantes par colonnes
  def count_missing_values(df: DataFrame, spark: SparkSession): DataFrame = {

    // Compteur des valeurs manquantes
    val missing_values = df.columns.map { colName =>
      val missingCount = df.filter(col(colName).isNull).count()
      Row(colName, missingCount)
    }

    // Définir le schéma du DataFrame
    val schema = StructType(List(
      StructField("Colonne", StringType, nullable = false),
      StructField("Valeurs manquantes", LongType, nullable = false)
    ))

    // Créer le DataFrame à partir des RDDs de Row et du schéma
    val missing_values_df = spark.createDataFrame(spark.sparkContext.parallelize(missing_values), schema)

    // Calculer le pourcentage
    val totalRows = df.count()
    val missing_values_with_percentage = missing_values_df.withColumn("Pourcentage manquant", col("Valeurs manquantes") / totalRows * 100)

    missing_values_with_percentage
  }

  // Fonction pour ajouter des préfixes aux colonnes d'un DataFrame
  private def addPrefixToColumns(df: DataFrame, prefix: String): DataFrame = {
    df.columns.foldLeft(df)((tempDF, colName) => tempDF.withColumnRenamed(colName, s"$prefix$colName"))
  }

  // Fonction pour corriger les données avec le flag 's' en utilisant la moyenne de la colonne dans la table weather
  def correctDataWithFlagSUsingMean(df: DataFrame): DataFrame = {

    // Liste des colonnes se terminant par 'Flag'
    val flagColumns = df.columns.filter(_.endsWith("Flag"))

    // Iteration sur les colonnes pour remplacer les valeurs 's' par la moyenne de la colonne principale
    var correctedDf = df
    for (colName <- flagColumns) {
      val mainCol = colName.stripSuffix("Flag")

      // Calcule de la moyenne de la colonne principale
      val colMean = correctedDf.select(mean(col(mainCol))).first().getDouble(0)

      // Remplacement des valeurs 's' par la moyenne
      correctedDf = correctedDf.withColumn(mainCol, when(col(colName) === "s", lit(colMean)).otherwise(col(mainCol)))}

    correctedDf
  }

  // Fonction pour créer la table FT
  def createFlightTable(df: DataFrame): DataFrame = {

    // Gestion des valeurs manquantes
    var cleanedDF = df
      .withColumn("CRS_DEP_TIME", lpad(col("CRS_DEP_TIME").cast(IntegerType).cast(StringType), 4, "0")) // Remplir avec 0 à gauche pour gérer les heures de type 1 ou 2 digits
      .withColumn("CRS_DEP_TIME", expr("substring(CRS_DEP_TIME, 1, 2) || ':' || substring(CRS_DEP_TIME, 3, 2)"))
      .withColumn("FL_DATE", to_date(col("FL_DATE"), "yyyy-MM-dd"))
      .withColumn("ARR_DELAY_NEW", col("ARR_DELAY_NEW").cast(DoubleType))
      .withColumn("CANCELLED", col("CANCELLED").cast(IntegerType))
      .withColumn("DIVERTED", col("DIVERTED").cast(IntegerType))
      .withColumn("CRS_ELAPSED_TIME", col("CRS_ELAPSED_TIME").cast(DoubleType))
      .withColumn("WEATHER_DELAY", col("WEATHER_DELAY").cast(DoubleType))
      .withColumn("NAS_DELAY", col("NAS_DELAY").cast(DoubleType))

    cleanedDF = cleanedDF
      .drop("Unnamed: 12") // Suppression de la colonne inutile
      .na.fill(0, Seq("ARR_DELAY_NEW", "WEATHER_DELAY", "NAS_DELAY"))
      .na.fill("", Seq("CRS_DEP_TIME", "FL_DATE"))

    cleanedDF = cleanedDF
      .withColumn("TIMESTAMP", to_timestamp(concat(col("FL_DATE"), lit(" "), col("CRS_DEP_TIME")), "yyyy-MM-dd HH:mm"))

    // Filtrage des vols détournés et annulés
    val filterDF = cleanedDF
      .where(col("DIVERTED") =!= 1 && col("CANCELLED") =!= 1) // On enlève les vols détournés et annulés
    // EST CE QU'ON GARDE CES DEUX COLONNES SACHANT Q'IU ELLES SONT EGALES A 0

    // Ajout d'un prefixe sur chaque colonne
    val newDF = addPrefixToColumns(filterDF, "FT_")

    newDF
      .coalesce(1) // Pour ne générer qu'un seul fichier CSV
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv("/Users/benjamin/Documents/GitHub/Flight/csvFiles/Flight") // Spécifier le chemin de sortie

    newDF
  }

  // Fonction pour créer la table OT
  def createObservationTable(df_1: DataFrame, df_2: DataFrame): DataFrame = {

    // Retraitement sur le colonne Time pour s'assurer qu'elle est bien sur un format HHmm
    val dfWithTime = df_1.withColumn("Time", lpad(col("Time"), 4, "0"))
    // Création du TimeStamp
    val dfWithTimeStamp = dfWithTime.withColumn("WEATHER_TIMESTAMP", to_timestamp(concat(col("DATE"), lit(" "), col("TIME")), "yyyy-MM-dd HHmm"))

    // Jointure entre wban et weather
    // On réalise une jointure "inner" qui ne conserve que les données communes et permet d'exclure toutes les données non liées à un aéroport
    // Utilisation de la fonction broadcast qui permet de diffuser la plus petite table sur tous les noeuds du cluster et éviter les mouvements de fichiers
    var newdf = dfWithTimeStamp.join(broadcast(df_2), Seq("WBAN"), "inner")

    // Ajout d'un prefixe sur chaque colonne
    newdf = addPrefixToColumns(newdf, "OT_")

    // Nettoyage des données flaggées avec 's' en utilisant la moyenne de la colonne principale
    newdf = correctDataWithFlagSUsingMean(newdf)

    newdf
  }




  // Fonction pour le filtrage des données
  def DF_GenerateFlightDataset(in_DF: DataFrame, in_DS_Type: String, in_DelayedThreshold: Int, in_OnTimeThreshold: Int = 0) : (DataFrame, DataFrame) = {

    // On vérifie que le seuil de retard soit bien supérieur au seuil de ponctualité
    if (in_DelayedThreshold < in_OnTimeThreshold) {
      throw new IllegalArgumentException(s"DF_GenerateDataset: error $in_DelayedThreshold delay threshold must be greater than $in_OnTimeThreshold on-time!")
    }

    // Filtrage des vols détournés et annulées
    val df_Filtered = in_DF.filter(col("DIVERTED") =!= 1 && col("CANCELLED") =!= 1)

    // Création des datasframes en fonction des filtres
    val out_delayed = in_DS_Type match {
      case "DS1" =>
        df_Filtered.filter(col("ARR_DELAY_NEW") >= in_DelayedThreshold && col("ARR_DELAY_NEW") === (col("WEATHER_DELAY") + col("NAS_DELAY")))
      case "DS2" =>
        df_Filtered.filter(col("ARR_DELAY_NEW") >= in_DelayedThreshold && (col("WEATHER_DELAY") > 0 || col("NAS_DELAY") >= in_DelayedThreshold))
      case "DS3" =>
        df_Filtered.filter(col("ARR_DELAY_NEW") >= in_DelayedThreshold && (col("WEATHER_DELAY") + col("NAS_DELAY") > 0))
      case "DS4" =>
        df_Filtered.filter(col("ARR_DELAY_NEW") >= in_DelayedThreshold)
      case _ =>
        throw new IllegalArgumentException(s"DF_GenerateDataset: error $in_DS_Type dataset type not allowed (DS1,DS2,DS3,DS4 only) !")
    }

    // Génération des deux dataframes de sortie
    val out_OnTime = df_Filtered.where(col("ARR_DELAY_NEW") <= in_OnTimeThreshold)

    val out_delayed_with_col: DataFrame = out_delayed.withColumn("OnTime", lit(false))
    val out_OnTime_with_col: DataFrame = out_OnTime.withColumn("OnTime", lit(true))

    // Sortie de la fonction
    (out_delayed_with_col, out_OnTime_with_col)

  }
}
