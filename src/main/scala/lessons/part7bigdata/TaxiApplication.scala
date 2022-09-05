package lessons.part7bigdata

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ Column, SparkSession }

object TaxiApplication extends App {

  val spark = SparkSession
    .builder()
    .config("spark.master", "local")
    .appName("Taxi Big Data Application")
    .getOrCreate()

  import spark.implicits._

  // val bigTaxiDF = spark.read
  //  .load("path/to/your/dataset/NYC_taxi_2009-2016.parquet")
  //  .cache()

  val taxiDF = spark.read
    .load("src/main/resources/data/yellow_taxi_jan_25_2018")
    .cache()

  taxiDF.printSchema()

  val taxiZonesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/taxi_zones.csv")
    .cache()

  taxiZonesDF.printSchema()

  /**
    * Questions:
    *
    * 1. Which zones have the most pickups/dropoffs overall?
    * 2. What are the peak hours for taxi?
    * 3. How are the trips distributed by length? Why are people taking the cab?
    * 4. What are the peak hours for long/short trips?
    * 5. What are the top 3 pickup/dropoff zones for long/short trips?
    * 6. How are people paying for the ride, on long/short trips?
    * 7. How is the payment type evolving with time?
    * 8. Can we explore a ride-sharing opportunity by grouping close short trips?
    *
    */

  // 1
  val pickupsByTaxiZoneDF = taxiDF
    .groupBy("PULocationID")
    .agg(count("*").as("totalTrips"))
    .join(taxiZonesDF, $"PULocationID" === $"LocationID")
    .drop("LocationID", "service_zone")
    .orderBy($"totalTrips".desc_nulls_last)

  // 1b - group by borough
  pickupsByTaxiZoneDF
    .groupBy($"Borough")
    .agg(sum($"totalTrips").as("totalTrips"))
    .orderBy($"totalTrips".desc_nulls_last)
    .show(false)

  // 2
  taxiDF
    .withColumn("hour_of_day", hour($"tpep_pickup_datetime"))
    .groupBy("hour_of_day")
    .agg(count("*").as("totalTrips"))
    .orderBy($"totalTrips".desc_nulls_last)
    .show(false)

  // 3
  val tripDistanceDF = taxiDF.select($"trip_distance".as("distance"))
  tripDistanceDF
    .select(
      count("*").as("count"),
      lit(30).as("threshold"),
      mean("distance").as("mean"),
      stddev("distance").as("stddev"),
      min("distance").as("min"),
      max("distance").as("max"),
    )
    .show(false)

  val tripsWithLengthDF = taxiDF.withColumn("isLong", $"trip_distance" >= 30)

  tripsWithLengthDF
    .groupBy("isLong")
    .count()
    .show(false)

  // 4
  tripsWithLengthDF
    .withColumn("hour_of_day", hour($"tpep_pickup_datetime"))
    .groupBy("hour_of_day", "isLong")
    .agg(count("*").as("totalTrips"))
    .orderBy($"totalTrips".desc_nulls_last)
    .show(false)

  // 5
  def pickupDropoffPopularity(predicate: Column) = tripsWithLengthDF
    .where(predicate)
    .groupBy("PULocationID", "DOLocationID")
    .agg(count("*").as("totalTrips"))
    .join(taxiZonesDF, $"PULocationID" === $"LocationID")
    .withColumnRenamed("Zone", "Pickup_Zone")
    .drop("LocationID", "Borough", "service_zone")
    .join(taxiZonesDF, $"DOLocationID" === $"LocationID")
    .withColumnRenamed("Zone", "Dropoff_Zone")
    .drop("LocationID", "Borough", "service_zone")
    .drop("PULocationID", "DOLocationID")
    .orderBy($"totalTrips".desc_nulls_last)

  // 6
  taxiDF
    .groupBy($"RatecodeID")
    .agg(count("*").as("totalTrips"))
    .orderBy($"totalTrips".desc_nulls_last)
    .show(false)

  // 7
  taxiDF
    .groupBy(to_date($"tpep_pickup_datetime").as("pickup_day"), $"RatecodeID")
    .agg(count("*").as("totalTrips"))
    .orderBy($"pickup_day")
    .show(false)

  // 8
  val passengerCountDF = taxiDF.where($"passenger_count" < 3).select(count("*"))
  passengerCountDF.show()
  taxiDF.select(count("*")).show(false)

  val groupAttemptsDF = taxiDF
    .select(
      round(unix_timestamp($"tpep_pickup_datetime") / 300).cast("integer").as("fiveMinId"),
      $"PULocationID",
      $"total_amount",
    )
    .where($"passenger_count" < 3)
    .groupBy($"fiveMinId", $"PULocationID")
    .agg(count("*").as("total_trips"), sum($"total_amount").as("total_amount"))
    .orderBy($"total_trips".desc_nulls_last)
    .withColumn("approximate_datetime", from_unixtime($"fiveMinId" * 300))
    .drop("fiveMinId")
    .join(taxiZonesDF, $"PULocationID" === $"LocationID")
    .drop("LocationID", "service_zone")

  val percentGroupAttempt   = 0.05
  val percentAcceptGrouping = 0.3
  val discount              = 5
  val extraCost             = 2
  val avgCostReduction      = 0.6 * taxiDF.select(avg($"total_amount")).as[Double].take(1)(0)

  val groupingEstimateEconomicImpactDF = groupAttemptsDF
    .withColumn("groupedRides", $"total_trips" * percentGroupAttempt)
    .withColumn(
      "acceptedGroupedRidesEconomicImpact",
      $"groupedRides" * percentAcceptGrouping * (avgCostReduction - discount),
    )
    .withColumn("rejectedGroupedRidesEconomicImpact", $"groupedRides" * (1 - percentAcceptGrouping) * extraCost)
    .withColumn("totalImpact", $"acceptedGroupedRidesEconomicImpact" + $"rejectedGroupedRidesEconomicImpact")

  groupingEstimateEconomicImpactDF
    .select(sum($"totalImpact").as("total"))
    .show(false)
  // 40k/day = 12 million/year!!!

}
