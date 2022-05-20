package com.sample.spark.essentials.dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {

  val spark = SparkSession
    .builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // Columns
  val firstColumn = carsDF.col("Name")

  // selecting (projecting)
  val carNamesDF = carsDF.select(firstColumn)

//  carNamesDF.show()

  // various select methods
  import spark.implicits._
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    Symbol("Year"), // Scala Symbol, auto-converted to column
    $"Horsepower",  // fancier interpolated string, returns a Column object
    expr("Origin")  // EXPRESSION
  )

  // select with plain column names
  carsDF.select("Name", "Year")

  // EXPRESSIONS
  val simplestExpression   = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  // selectExpr
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // DF processing

  // adding a column
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)

  // renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")

  // careful with column names
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")

  // remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // filtering
  val europeanCarsDF  = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")

  // filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA'")

  // chain filters
  val americanPowerfulCarsDF  = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  // unioning = adding more rows
  val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
  val allCarsDF  = carsDF.union(moreCarsDF) // works if the DFs have the same schema

  // distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()
//  allCountriesDF.show()

  /**
   * Exercises
   *
   * 1. Read the movies DF and select 2 columns of your choice
   * 2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
   * 3. Select all COMEDY movies with IMDB rating above 6
   *
   * Use as many versions as possible
   */

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // 1
  val projectionDF = moviesDF
    .select(col("Title"), col("Major_Genre"))
    .filter(col("Major_Genre").isNotNull)

//  projectionDF.show()

  // 2
  // "US_Gross":37600000,"Worldwide_Gross":37600000,"US_DVD_Sales":null
  val withTotalProfitDF = moviesDF
    .select(
      $"Title",
      $"US_Gross",
      $"Worldwide_Gross",
      $"US_DVD_Sales",
      expr("US_Gross + Worldwide_Gross + US_DVD_Sales").as("TotalProfit")
    )
    .where($"US_DVD_Sales".isNotNull)

//  withTotalProfitDF.show()

  // 3
  val goodComediesDF = moviesDF
    .select($"Title", $"Major_Genre", $"IMDB_Rating")
    .where($"Major_Genre" === "Comedy" and $"IMDB_Rating" > 6)

  goodComediesDF.show()
}
