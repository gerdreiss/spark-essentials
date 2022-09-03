package lessons.part4sql

import commons.Utils._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.io.File

object SparkSql extends App {

  val warehouseDir = "src/main/resources/warehouse"
  val spark = SparkSession
    .builder()
    .appName("Spark SQL Practice")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", warehouseDir)
    // only for Spark 2.4 users:
    // .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")
    .cache()

  // regular DF API
  carsDF.select(col("Name")).where(col("Origin") === "USA")

  // use Spark SQL
  carsDF.createOrReplaceTempView("cars")
  spark.sql("select Name from cars where Origin = 'USA'").show(false)

  // we can run ANY SQL statement
  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")
  spark.sql("show databases").show(false)

  // transfer tables from a DB to Spark tables
  val driver   = "org.postgresql.Driver"
  val url      = "jdbc:postgresql://localhost:5432/rtjvm"
  val user     = "docker"
  val password = "docker"

  def readTable(tableName: String): DataFrame =
    spark.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", s"public.$tableName")
      .load()

  def readTable2(tableName: String): DataFrame =
    spark.read.jdbc(
      url,
      s"public.$tableName",
      Map(
        "driver"   -> driver,
        "user"     -> user,
        "password" -> password,
      ).toJavaProperties,
    )

  def transferTables(tableNames: List[String], shouldWriteToWarehouse: Boolean = false): Unit =
    tableNames.foreach { tableName =>
      val tableDF = readTable2(tableName)
      tableDF.createOrReplaceTempView(tableName)

      if (shouldWriteToWarehouse) {
        tableDF.write
          .mode(SaveMode.Overwrite)
          .saveAsTable(tableName)
      }
    }

  transferTables(List("employees", "departments", "titles", "dept_emp", "salaries", "dept_manager"))

  // read DF from loaded Spark tables
  spark.read.table("employees").show(false)
  spark.read.table("departments").show(false)
  spark.read.table("titles").show(false)

  /**
    * Exercises
    *
    * 1. Read the movies DF and store it as a Spark table in the rtjvm database.
    * 2. Count how many employees were hired in between Jan 1 1999 and Jan 1 2000.
    * 3. Show the average salaries for the employees hired in between those dates, grouped by department.
    * 4. Show the name of the best-paying department for employees hired in between those dates.
    */

  // 1
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")
    .cache()

  FileUtils.deleteDirectory(new File(warehouseDir))

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("movies")

  // 2
  spark
    .sql(
      """
      |select count(*)
      |  from employees
      | where hire_date > '1999-01-01' and hire_date < '2000-01-01'
    """.stripMargin
    )
    .show(false)

  // 3
  spark
    .sql(
      """
      |select de.dept_no, avg(s.salary)
      |  from employees e, dept_emp de, salaries s
      | where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      |   and e.emp_no = de.emp_no
      |   and e.emp_no = s.emp_no
      | group by de.dept_no
    """.stripMargin
    )
    .show(false)

  // 4
  spark
    .sql(
      """
      |select avg(s.salary) payments, d.dept_name
      |  from employees e, dept_emp de, salaries s, departments d
      | where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      |   and e.emp_no = de.emp_no
      |   and e.emp_no = s.emp_no
      |   and de.dept_no = d.dept_no
      | group by d.dept_name
      | order by payments desc
      | limit 1
    """.stripMargin
    )
    .show(false)
}
