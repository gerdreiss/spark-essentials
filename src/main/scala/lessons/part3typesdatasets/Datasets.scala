package lessons.part3typesdatasets

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

import java.sql.Date

object Datasets extends App {

  val spark = SparkSession
    .builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF: DataFrame = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")
    .cache()

  numbersDF.printSchema()

  // convert a DF to a Dataset
  implicit val intEncoder: Encoder[Int] = Encoders.scalaInt

  val numbersDS: Dataset[Int] = numbersDF.as[Int]

  // dataset of a complex type
  // 1 - define your case class
  case class Car(
      Name: String,
      Miles_per_Gallon: Option[Double],
      Cylinders: Long,
      Displacement: Double,
      Horsepower: Option[Long],
      Weight_in_lbs: Long,
      Acceleration: Double,
      Year: Date,
      Origin: String,
  )

  // 2 - read the DF from the file
  def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")
    .cache()

  val carsDF = readDF("cars.json")
    .withColumn("Year", col("Year").cast(DateType))

  // 3 - define an encoder (importing the implicits)
  import spark.implicits._
  // implicit val carEncoder: Encoder[Car] = Encoders.product[Car]

  // 4 - convert the DF to DS
  val carsDS = carsDF.as[Car]

  // DS collection functions
  numbersDS.filter(_ < 100)

  // map, flatMap, fold, reduce, for comprehensions ...
  carsDS.map(_.Name.toUpperCase()).show(false)

  /**
    * Exercises
    *
    *   1. Count how many cars we have
    *   2. Count how many POWERFUL cars we have (HP > 140)
    *   3. Average HP for the entire dataset
    */

  // 1
  val carsCount = carsDS.count
  println(carsCount)

  // 2
  println(carsDS.filter(_.Horsepower.exists(_ > 140)).count)

  // 3
  println(carsDS.flatMap(_.Horsepower).reduce(_ + _) / carsCount)

  // also use the DF functions!
  carsDS.select(avg(col("Horsepower")))

  // Joins
  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS       = readDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS         = readDF("bands.json").as[Band]

  guitarPlayersDS
    .joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"), "inner")
    .show(false)

  /**
    * Exercise:
    * join the guitarsDS and guitarPlayersDS, in an outer join (hint: use array_contains)
    */

  guitarPlayersDS
    .joinWith(guitarsDS, array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")), "outer")
    .show(false)

  // Grouping DS

  carsDS
    .groupByKey(_.Origin)
    .count()
    .show(false)

  // joins and groups are WIDE transformations, will involve SHUFFLE operations

}
