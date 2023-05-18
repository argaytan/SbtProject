package org.arg.globant

import org.apache.spark.sql.{SaveMode, SparkSession}
import com.arg.common.utilities.UtilFunctions
import org.apache.log4j.Logger
import java.util.Properties

object CsvToAzureSql {
  def main(args: Array[String]): Unit = {
    val utilFunctions: UtilFunctions = new UtilFunctions()
    val logger: Logger = Logger.getRootLogger
    val props: Properties = getProperties

    logger.warn("Starting CsvToAzureSql Application")
    val spark = SparkSession.builder()
      .appName("CsvToAzureSql")
      .config(utilFunctions.getSparkConf)
      .getOrCreate()


    // Configure Azure SQL Server connection properties
    logger.warn("Configure Azure SQL Server connection properties")
    val jdbcHostname = props.get("jdbcHostname").toString
    val jdbcDatabase = props.get("jdbcDatabase").toString
    val jdbcDriver = props.get("jdbcDriver").toString
    val jdbcUsername = props.get("jdbcUser").toString
    val jdbcPassword = props.get("jdbcPassword").toString
    val jdbcUrl = s"jdbc:sqlserver://$jdbcHostname:1433;database=$jdbcDatabase;"


    val connectionProperties: Properties = new Properties()
    connectionProperties.setProperty("user", jdbcUsername)
    connectionProperties.setProperty("password", jdbcPassword)
    connectionProperties.setProperty("driver", jdbcDriver)

    // Database Tables
    val schema = "common"
    val tableList = Map(
      "hired_employees" -> "'id', 'name', 'datetime', 'department_id', 'job_id'",
      "jobs" -> "'id', 'job'",
      "departments" -> "'id', 'department'"
    )

    // Iterate over the table list to overwrite data in Azure SQL Database
    for ((tableName, f) <- tableList) {
      println(s"table name: $tableName")
      println(s"fields: $f")

      //Create column name type variables
      val fields = Seq(f.split(","): _*)

      // Load CSV data into a DataFrame and add column names
      logger.warn("Load CSV data into a DataFrame and add column names")
      val df = spark.read
        .format("csv")
        .option("header", "false")
        .option("inferSchema", "true")
        .load("src/main/resources/" + tableName + ".csv")
        .toDF(fields: _*)  // add column names
      df.printSchema()
      df.show(false)

      // Write the DataFrame to the table inAzure SQL Database
      logger.warn("Write the DataFrame to the table inAzure SQL Database")
      val saveMode = SaveMode.Overwrite
      df.write.mode(saveMode)
        .jdbc(jdbcUrl, schema + "." + tableName, connectionProperties)

    }
    spark.stop()

  }


  def getProperties: Properties = {
    val prop: Properties = new Properties()
    prop.put("jdbcHostname", sys.env.getOrElse("AZURE_SQL_URL", ""))
    prop.put("jdbcDriver", sys.env.getOrElse("AZURE_SQL_DRIVER", ""))
    prop.put("jdbcDatabase", sys.env.getOrElse("AZURE_SQL_DB", ""))
    prop.put("jdbcUser", sys.env.getOrElse("AZURE_SQL_USER", ""))
    prop.put("jdbcPassword", sys.env.getOrElse("AZURE_SQL_PASSWORD", ""))
    prop
  }
}

