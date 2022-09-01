package lessons.part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {

  val spark = SparkSession
    .builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")
    .cache()

  // Dates

  val titleCol       = col("Title")
  val releaseDateCol = to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release")

  val moviesWithReleaseDates = moviesDF.select(titleCol, releaseDateCol) // conversion

  moviesWithReleaseDates
    .withColumn("Today", current_date())
    .withColumn("Right_Now", current_timestamp())
    .withColumn("Movie_Age", round(datediff(col("Today"), col("Actual_Release")) / 365, 2)) // date_add, date_sub
    .show(false)

  moviesWithReleaseDates.select("*").where(col("Actual_Release").isNull)

  /** Exercise
    * 1. How do we deal with multiple date formats?
    * 2. Read the stocks DF and parse the dates
    */

  // 1 - parse the DF multiple times, then union the small DFs

  // 2
  val stocksDF = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/data/stocks.csv")
    .cache()

  stocksDF
    .withColumn("actual_date", to_date(col("date"), "MMM dd yyyy"))
    .show(false)

  // Structures

  // 1 - with col operators
  moviesDF
    .select(titleCol, struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(titleCol, col("Profit").getField("US_Gross").as("US_Profit"))
    .show(false)

  // 2 - with expression strings
  moviesDF
    .selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")

  // Arrays

  val moviesWithWords = moviesDF.select(titleCol, split(titleCol, " |,").as("Title_Words")) // ARRAY of strings

  moviesWithWords
    .select(
      titleCol,
      col("Title_Words"),
      expr("Title_Words[0]"), // indexing
      size(col("Title_Words")), // array size
      array_contains(col("Title_Words"), "Love"),// look for value in array
    )
    .show(false)

}
