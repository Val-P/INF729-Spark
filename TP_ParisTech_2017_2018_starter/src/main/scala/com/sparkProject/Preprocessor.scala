package com.sparkProject

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
//import org.apache.spark.implicits._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.IntegerType

object Preprocessor {

  def main(args: Array[String]): Unit = {

    // Des réglages optionels du job spark. Les réglages par défaut fonctionnent très bien pour ce TP
    // on vous donne un exemple de setting quand même
    val conf = new SparkConf().setAll(Map(
      "spark.scheduler.mode" -> "FIFO",
      "spark.speculation" -> "false",
      "spark.reducer.maxSizeInFlight" -> "48m",
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      "spark.kryoserializer.buffer.max" -> "1g",
      "spark.shuffle.file.buffer" -> "32k",
      "spark.default.parallelism" -> "12",
      "spark.sql.shuffle.partitions" -> "12"
    ))

    // Initialisation de la SparkSession qui est le point d'entrée vers Spark SQL (donne accès aux dataframes, aux RDD,
    // création de tables temporaires, etc et donc aux mécanismes de distribution des calculs.)
    val spark = SparkSession
      .builder
      .config(conf)
      .appName("TP_spark")
      .getOrCreate()


    /*******************************************************************************
      *
      *       TP 2
      *
      *       - Charger un fichier csv dans un dataFrame
      *       - Pre-processing: cleaning, filters, feature engineering => filter, select, drop, na.fill, join, udf, distinct, count, describe, collect
      *       - Sauver le dataframe au format parquet
      *
      *       if problems with unimported modules => sbt plugins update
      *
      ********************************************************************************/

    println("hello world ! from Preprocessor")

    // 1. Chargement des data

    // a) Charger un csv dans dataframe
    val df: DataFrame = spark
      .read
      .option("header", true)  // Use first line of all files as header
      .option("inferSchema", "true") // Try to infer (déduire) the data types of each column
      .option("nullValue", "")  // replace "" by null
      .option("nullValue", "false")  // replace strings "false" (that indicates missing data) by null values
      .csv("/Users/valentinphetchanpheng/Documents/MS BGD Telecom ParisTech/TP Hadoop:Spark/Spark/Data/train.csv")

    // b) nombre de lignes et colonnes
    println(s"Total number of rows: ${df.count}")
    println(s"Number of columns: ${df.columns.length}")

    // c) afficher les lignes
    df.show(20)

    // d) afficher structure table
    //df.printSchema()

    // e) modifier structure colonne (assigner type Int aux colonnes)
    val df2 = df
      .withColumn("goal", df("goal").cast(IntegerType)) /** autre méthode : df("goal").cast("int") **/
      .withColumn("deadline" , df("deadline").cast(IntegerType))
      .withColumn("state_changed_at", df("state_changed_at").cast(IntegerType))
      .withColumn("created_at", df("created_at").cast(IntegerType))
      .withColumn("launched_at", df("launched_at").cast(IntegerType))
      .withColumn("backers_count", df("backers_count").cast(IntegerType))
      .withColumn("final_status", df("final_status").cast(IntegerType))

    //df2.printSchema()
    df2.describe().show()


    // 2. Cleaning
    // a) afficher description stat
    df2.select(mean("goal")).show()

    // b) cleaning
    df2.groupBy("country").count.show()

  }

}
