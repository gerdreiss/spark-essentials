package exercises

import org.apache.spark.sql.SparkSession

object DataFramesBasics extends App {

  // creating a SparkSession
  val spark = SparkSession
    .builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  /** Exercise:
    * 1) Create a manual DF describing smartphones
    *   - make
    *   - model
    *   - screen dimension
    *   - camera megapixels
    */
  Seq(
    ("Samsung", "Galaxy S10", "Android", 12),
    ("Apple", "iPhone X", "iOS", 13),
    ("Nokia", "3310", "THE BEST", 0),
  )
    .toDF("Make", "Model", "Platform", "CameraMegapixels")
    .show()

  /** 2) Read another file from the data/ folder, e.g. movies.json
    *   - print its schema
    *   - count the number of rows, call count()
    */
  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")
    .cache()

  val schema = moviesDF.schema.treeString
  println(s"${Console.BLUE_B}The Movies DF schema is:\n$schema${Console.RESET}")

  val rowCount: Long = moviesDF.count()
  println(s"${Console.BLUE_B}The Movies DF has $rowCount rows${Console.RESET}")

}
