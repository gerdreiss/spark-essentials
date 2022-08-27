package lessons.part2dataframes

import org.apache.spark.sql.types._

object Schemas {
  val carsSchema: StructType =
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

}
