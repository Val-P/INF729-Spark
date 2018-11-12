package object sparkProject {


  import org.apache.spark.SparkConf
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

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
        .master("local")
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

      val df = spark
        .read
        .option("header", true)  // Use first line of all files as header
        .option("inferSchema", "true") // Try to infer the data types of each column
        .option("nullValue", "")
        .option("nullValue", "false")  // replace strings "false" (that indicates missing data) by null values
        .csv("/Users/stephanepeillet/Downloads/train_clean.csv")

      // b) nombre de lignes et colonnes
      println(s"Total number of rows: ${df.count}")
      println(s"Number of columns ${df.columns.length}")

      // c) Observer le dataframe: First 20 rows, and all columns :
      df.head(20)

      // d) Le schema donne le nom et type (string, integer,...) de chaque colonne
      df.printSchema()

      // e) Assigner le bon type aux colonnes
      val dfCasted = df
        .withColumn("goal", df("goal").cast("Int"))
        .withColumn("deadline" , df("deadline").cast("Int"))
        .withColumn("state_changed_at", df("state_changed_at").cast("Int"))
        .withColumn("created_at", df("created_at").cast("Int"))
        .withColumn("launched_at", df("launched_at").cast("Int"))
        .withColumn("backers_count", df("backers_count").cast("Int"))
        .withColumn("final_status", df("final_status").cast("Int"))
      dfCasted.printSchema()

      dfCasted.describe().show()
    }

  }



}
