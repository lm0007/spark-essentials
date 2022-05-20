package com.sample.spark.essentials.dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

object DataSources extends App {

  val spark = SparkSession
    .builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(
    Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", DateType),
      StructField("Origin", StringType)
    )
  )

  /*
    Reading a DF:
    - format
    - schema or inferSchema = true
    - path
    - zero or more options
   */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)         // enforce a schema
    .option("mode", "failFast") // dropMalformed, permissive (default)
    .option("path", "src/main/resources/data/cars.json")
    .load()

  // alternative reading with options map
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(
      Map(
        "mode"        -> "failFast",
        "path"        -> "src/main/resources/data/cars.json",
        "inferSchema" -> "true"
      )
    )
    .load()

  /*
   Writing DFs
   - format
   - save mode = overwrite, append, ignore, errorIfExists
   - path
   - zero or more options
   */
//  carsDF.write
//    .format("json")
//    .mode(SaveMode.Overwrite)
//    .save("src/main/resources/data/cars_dupe.json")

  // JSON flags
  spark.read
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd") // couple with schema; if Spark fails parsing, it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")

  // CSV flags
  val stocksSchema = StructType(
    Array(
      StructField("symbol", StringType),
      StructField("date", DateType),
      StructField("price", DoubleType)
    )
  )

  spark.read
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")

  // write to parquet
//  carsDF.write
//    .mode(SaveMode.Overwrite)
//    .parquet("src/main/resources/data/cars_parq.parquet")

  // Text files
//  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  // Reading from a remote DB
  val driver   = "org.postgresql.Driver"
  val url      = "jdbc:postgresql://localhost:5432/rtjvm"
  val user     = "docker"
  val password = "docker"

  val employeesDF = spark.read
    .format("jdbc")
    .options(
      Map(
        "driver"   -> driver,
        "url"      -> url,
        "user"     -> user,
        "password" -> password,
        "dbtable"  -> "public.employees"
      )
    )
    .load()

//  employeesDF.show()

  /**
   * Exercise: read the movies DF, then write it as
   * - tab-separated values file
   * - snappy Parquet
   * - table "public.movies" in the Postgres DB
   */

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // write as tab separated values
  moviesDF.write
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .option("sep", "\t")
    .csv("src/main/resources/data/movies_tab_sep.csv")

  // write as parquet
//  moviesDF.write
//    .mode(SaveMode.Overwrite)
//    .parquet("src/main/resources/data/movies_par.parquet")

  // write to postgresql
//  moviesDF.write
//    .format("jdbc")
//    .options(
//      Map(
//        "driver"   -> driver,
//        "url"      -> url,
//        "user"     -> user,
//        "password" -> password,
//        "dbtable"  -> "public.movies_out"
//      )
//    )
//    .save()
}
