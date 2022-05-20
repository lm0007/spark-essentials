package com.sample.spark.essentials.dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object DataFramesBasics extends App {

  val spark = SparkSession
    .builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  val firstDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

//  firstDF.show()
//  firstDF.printSchema()

//  firstDF.take(10).foreach(println)

  val carsSchema = StructType(
    Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", StringType),
      StructField("Origin", StringType)
    )
  )

  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsSchema)
    .load("src/main/resources/data/cars.json")

//  carsDFWithSchema.show()

  // create rows by hand
  val myRow = Row("chevrolet chevelle malibu", 18, 8, 307, 130, 3504, 12.0, "1970-01-01", "USA")

  // create DF from tuples
  val cars = Seq(
    ("chevrolet chevelle malibu", 18, 8, 307, 130, 3504, 12.0, "1970-01-01", "USA"),
    ("buick skylark 320", 15, 8, 350, 165, 3693, 11.5, "1970-01-01", "USA"),
    ("plymouth satellite", 18, 8, 318, 150, 3436, 11.0, "1970-01-01", "USA"),
    ("amc rebel sst", 16, 8, 304, 150, 3433, 12.0, "1970-01-01", "USA"),
    ("ford torino", 17, 8, 302, 140, 3449, 10.5, "1970-01-01", "USA"),
    ("ford galaxie 500", 15, 8, 429, 198, 4341, 10.0, "1970-01-01", "USA"),
    ("chevrolet impala", 14, 8, 454, 220, 4354, 9.0, "1970-01-01", "USA"),
    ("plymouth fury iii", 14, 8, 440, 215, 4312, 8.5, "1970-01-01", "USA"),
    ("pontiac catalina", 14, 8, 455, 225, 4425, 10.0, "1970-01-01", "USA"),
    ("amc ambassador dpl", 15, 8, 390, 190, 3850, 8.5, "1970-01-01", "USA")
  )
  val manualCarsDF = spark.createDataFrame(cars) // schema auto-inferred

  // note: DFs have schemas, rows do not

  // create DFs with implicits
  import spark.implicits._
  val manualCarsDFWithImplicits = cars.toDF(
    "Name",
    "MPG",
    "Cylinders",
    "Displacement",
    "HP",
    "Weight",
    "Acceleration",
    "Year",
    "CountryOrigin"
  )

//  manualCarsDF.printSchema()
//  manualCarsDFWithImplicits.printSchema()

  /**
   * Exercise:
   * 1) Create a manual DF describing smartphones
   *   - make
   *   - model
   *   - screen dimension
   *   - camera megapixels
   *
   * 2) Read another file from the data/ folder, e.g. movies.json
   *   - print its schema
   *   - count the number of rows, call count()
   */

  // 1
  val smartPhones = Seq(
    ("Apple", "iPhone 13", "200x300", "10mpx"),
    ("Samsung", "some model 1", "200x300", "10mpx"),
    ("LG", "some model 2", "200x300", "10mpx"),
    ("Google", "some model 3", "200x300", "10mpx")
  )

  val smartPhonesDF = spark.createDataFrame(smartPhones)
//  smartPhonesDF.printSchema()

  // 2
  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  moviesDF.printSchema()
  moviesDF.show()
}
