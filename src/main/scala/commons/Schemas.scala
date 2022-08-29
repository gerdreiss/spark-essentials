package commons

import org.apache.spark.sql.types._

import java.time.LocalDate

object Schemas {
  val cars: StructType =
    StructType(
      Array(
        StructField("Name", StringType),
        StructField("Miles_per_Gallon", DoubleType),
        StructField("Cylinders", LongType),
        StructField("Displacement", DoubleType),
        StructField("Horsepower", LongType),
        StructField("Weight_in_lbs", LongType),
        StructField("Acceleration", DoubleType),
        StructField("Year", DateType),
        StructField("Origin", StringType),
      )
    )

  case class Car(
      name: String,
      mpg: Option[Double],
      cylinders: Option[Long],
      displacement: Option[Double],
      horsepower: Option[Long],
      weight: Option[Long],
      acceleration: Option[Double],
      year: LocalDate,
      origin: String,
  )
}
