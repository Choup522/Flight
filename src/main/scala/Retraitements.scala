import org.apache.spark.sql.{DataFrame,SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, StringType, LongType}

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

  // Fonction pour créer la table FT
  def createFlightTable(df: DataFrame): DataFrame = {

    // FAIRE AUSSI LES RETRAITEMENTS , CORRECTION DES VALEURS MANQUANTES ETC ==> IL NE SEMBLE PAS Y EN AVOIR SUR LES DONNEES SAMPLE, A CONFIRMER AVEC TOUTES LES DONNEES

    val cleanedDF = df
      .drop("Unnamed: 12") // Suppression de la colonne inutile
      .na.replace("CANCELLED", Map("0.0" -> "0")) // Ajustement des données de la colonne CANCELLED (VERIFIER SI ON EST PAS SUR DES INT PLUTOT ==> LIEN SCHEMA)
      .na.replace("DIVERTED", Map("0.0" -> "0")) // Ajustement des données de la colonne DIVERTED (VERIFIER SI ON EST PAS SUR DES INT PLUTOT ==> LIEN SCHEMA)
      .where(col("DIVERTED") =!= 1 && col("CANCELLED") =!= 1) // On enlève les vols détournés et annulés
      .withColumn("ARR_DELAY_NEW", when(col("ARR_DELAY_NEW").isNull, 0).otherwise(col("ARR_DELAY_NEW"))) // Remplacer les valeurs nulles par 0
      .withColumn("CRS_DEP_TIME", lpad(col("CRS_DEP_TIME"), 4, "0")) // Ajout de 0 pour les heures de moins de 4 chiffres
      .withColumn("FLIGHT_TIMESTAMP", to_timestamp(concat(col("FL_DATE"), lit(" "), col("CRS_DEP_TIME")), "yyyy-MM-dd HHmm"))
    //.withColumn("CRS_DEP_TIME", regexp_replace(col("CRS_DEP_TIME"), ".", "")) // Supprimer les points dans la colonne CRS_DEP_TIME

    // Ajout d'un prefixe sur chaque colonne
    val newdf = addPrefixToColumns(cleanedDF, "FT_")

    newdf
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
