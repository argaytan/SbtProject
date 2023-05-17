package org.arg.globant

import org.apache.spark.sql.{SaveMode, SparkSession}
import com.arg.common.utilities.UtilFunctions
import org.apache.log4j.Logger

object CsvToAzureSql {
  def main(args: Array[String]): Unit = {
    val utilFunctions: UtilFunctions = new UtilFunctions()
    val logger: Logger = Logger.getRootLogger()

    logger.warn("Starting CsvToAzureSql Application")
    val spark = SparkSession.builder()
      .appName("CsvToAzureSql")
      .config(utilFunctions.getSparkConf)
      .getOrCreate()

    // Configure Azure SQL Server connection properties
    val jdbcHostname = "testarg.database.windows.net"
    val jdbcDatabase = "testdb"
    val jdbcUrl = s"jdbc:sqlserver://$jdbcHostname:1433;database=$jdbcDatabase;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;authentication=ActiveDirectoryPassword"
    val jdbcUsername = "dev1"
    val jdbcPassword = "xxxxxx"  // todo add env variable
    val jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

    // Load CSV data into a DataFrame and add column names
    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/hired_employees.csv")
      .toDF("employee_id", "name", "date_hired", "department", "job")  // add column names
    df.printSchema()
    df.show(false)

    // Write DataFrame to Azure SQL Server
    df.write
      .format("jdbc")
      .mode(SaveMode.Append)
      .option("url", jdbcUrl)
      .option("dbtable", "common.employees")
      .option("user", jdbcUsername)
      .option("password", jdbcPassword)
      .option("driver", jdbcDriver)
      .save()

    spark.stop()
  }
}
