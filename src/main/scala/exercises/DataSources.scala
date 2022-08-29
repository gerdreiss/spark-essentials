package exercises

import commons.Schemas
import commons.Utils._
import org.apache.spark.sql._

import java.time.{ LocalDate, Month }

object DataSources extends App {

  val spark = SparkSession
    .builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val carsDS: Dataset[Schemas.Car] = spark.read
    .schema(Schemas.cars)                // enforce a schema
    .option("dateFormat", "YYYY-MM-dd")  // parse dates in this format
    .option("allowSingleQuotes", "true") // allow single quotes in JSON
    .option("mode", "failFast")          // dropMalformed, permissive (default)
    .json("src/main/resources/data/cars.json")
    // .filter(col("Origin") =!= "USA") // filter out American cars
    // .filter(col("Year") > lit("1979-12-31")) // filter out cars made in or after 1980
    .withColumnRenamed("Name", "name")
    .withColumnRenamed("Miles_per_Gallon", "mpg")
    .withColumnRenamed("Cylinders", "cylinders")
    .withColumnRenamed("Displacement", "displacement")
    .withColumnRenamed("Horsepower", "horsepower")
    .withColumnRenamed("Weight_in_lbs", "weight")
    .withColumnRenamed("Acceleration", "acceleration")
    .withColumnRenamed("Year", "year")
    .withColumnRenamed("Origin", "origin")
    .as[Schemas.Car] // convert to a strongly-typed case class
    .filter(_.origin != "USA")                                      // filter out American cars
    .filter(_.year.isAfter(LocalDate.of(1979, Month.DECEMBER, 31))) // filter out cars made after the year 1979
    // .sort($"Horsepower".desc) // sort by weight
    .orderBy($"horsepower".desc)                                    // sort by weight
    .cache()

  carsDS.show(carsDS.count().intValue(), truncate = false)

  spark.read
    .jdbc(
      "jdbc:postgresql://localhost:5432/rtjvm",
      "public.employees",
      Map(
        "driver"   -> "org.postgresql.Driver",
        "user"     -> "docker",
        "password" -> "docker",
      ).toJavaProperties,
    )
    .show()

  val moviesDF: DataFrame = spark.read
    .json("src/main/resources/data/movies.json")
    .cache()

  moviesDF.write
    .mode("overwrite")
    .jdbc(
      "jdbc:postgresql://localhost:5432/rtjvm",
      "public.movies",
      Map(
        "driver"   -> "org.postgresql.Driver",
        "user"     -> "docker",
        "password" -> "docker",
      ).toJavaProperties,
    )

  spark.read
    .jdbc(
      "jdbc:postgresql://localhost:5432/rtjvm",
      "public.movies",
      Map(
        "driver"   -> "org.postgresql.Driver",
        "user"     -> "docker",
        "password" -> "docker",
      ).toJavaProperties,
    )
    .show(false)

  moviesDF.write
    .mode("overwrite")
    .option("header", "true")
    .option("sep", "\t")
    .csv("src/main/resources/data/movies.csv")

  spark.read
    .csv("src/main/resources/data/movies.csv")
    .show(false)

}
