package lessons.part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ col, column, expr }

object ColumnsAndExpressions extends App {

  val spark = SparkSession
    .builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")
    .cache()

  // Columns
  val nameColumn = carsDF.col("Name")

  // selecting (projecting)
  carsDF.select(nameColumn).show(false)

  // various select methods
  import spark.implicits._
  carsDF
    .select(
      carsDF.col("Name"),
      col("Acceleration"),
      column("Weight_in_lbs"),
      Symbol("Year"), // deprecated -> 'Year, // Scala Symbol, auto-converted to column
      $"Horsepower",  // fancier interpolated string, returns a Column object
      expr("Origin"), // EXPRESSION
    )
    .show(false)

  // select with plain column names
  carsDF.select("Name", "Year")

  // EXPRESSIONS
  val simplestExpression   = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  carsDF
    .select(
      col("Name"),
      col("Weight_in_lbs"),
      weightInKgExpression.as("Weight_in_kg"),
      expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2"),
    )
    .show(false)

  // selectExpr
  carsDF
    .selectExpr(
      "Name",
      "Weight_in_lbs",
      "Weight_in_lbs / 2.2",
    )
    .show(false)

  // DF processing

  // adding a column
  carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2).show(false)
  // renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  // careful with column names
  carsWithColumnRenamed.selectExpr("`Weight in pounds`").show(false)
  // remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement").show(false)

  // filtering
  carsDF.filter(col("Origin") =!= "USA")
  carsDF.where(col("Origin") =!= "USA")
  // filtering with expression strings
  carsDF.filter("Origin = 'USA'")
  // chain filters
  carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  carsDF.filter("Origin = 'USA' and Horsepower > 150")

  // unioning = adding more rows
  val moreCarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/more_cars.json")

  carsDF.union(moreCarsDF).show(false) // works if the DFs have the same schema

  // distinct values
  carsDF.select("Origin").distinct().show(false)

  /** Exercises
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
    .cache()

  moviesDF.show(false)

  // 1
  moviesDF.select("Title", "Release_Date")
  moviesDF.select(
    moviesDF.col("Title"),
    col("Release_Date"),
    $"Major_Genre",
    expr("IMDB_Rating"),
  )
  moviesDF.selectExpr(
    "Title",
    "Release_Date",
  )

  // 2
  moviesDF.select(
    col("Title"),
    col("US_Gross"),
    col("Worldwide_Gross"),
    col("US_DVD_Sales"),
    (col("US_Gross") + col("Worldwide_Gross")).as("Total_Gross"),
  )

  moviesDF.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_Gross + Worldwide_Gross as Total_Gross",
  )

  moviesDF
    .select("Title", "US_Gross", "Worldwide_Gross")
    .withColumn("Total_Gross", col("US_Gross") + col("Worldwide_Gross"))

  // 3
  moviesDF
    .select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)

  moviesDF
    .select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy")
    .where(col("IMDB_Rating") > 6)

  moviesDF
    .select("Title", "IMDB_Rating")
    .where("Major_Genre = 'Comedy' and IMDB_Rating > 6")
    .show(false)
}
