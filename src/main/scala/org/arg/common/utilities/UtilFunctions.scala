package com.arg.common.utilities

import java.io.{ FileNotFoundException, IOException }
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{ col, current_timestamp, lit, regexp_replace, _ }
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{ Column, DataFrame, SparkSession }

import scala.io.Source
import scala.util.Try

object UtilFunctions {
  private val NETEZZA = "netezza"
}

class UtilFunctions {
  /**
   * Loads the properties contained in the Properties file specified @path
   *
   * @author Alberto Gaytan
   * @param path Properties file location
   * @example loadPropertiesFile("common/src/main/resources/commercial.prod.properties")
   * @return Properties file with data coming from the file @path
   */
  def loadPropertiesFile(path: String): Properties = {
    val prop: Properties = new Properties()
    try {
      val fullPath = getClass.getResourceAsStream(path)
      prop.load(Source.fromInputStream(fullPath).bufferedReader())
      prop
    } catch {
      case e: FileNotFoundException =>
        throw new FileNotFoundException("[ERROR]: Exception: File missing.\n Error description:%s".format(e))
      case e: IOException =>
        throw new IOException("[ERROR]: Input/output Exception.\n Error description:%s".format(e))
      case unknown: Exception =>
        throw new Exception("[ERROR]: Error loading the properties.\n Details:%s".format(unknown))
    }
  }

  /**
   * Cast data in the DataFrame specified
   *
   * @author Alberto Gaytan
   * @param df DataFrame with columns to cast
   * @return Dataframe with bad data removed
   */
  def castData()(df: DataFrame): DataFrame = {
    val dfResult = df
      .transform(truncateMilliseconds())
    println("[INFO]: Data Casted Successfully.")
    dfResult
  }

  /**
   * Cleans bad data in the DataFrame specified
   *
   * @author Alberto Gaytan
   * @param df DataFrame to clean
   * @return Dataframe with bad data removed
   */
  def cleanBadData(database: String)(df: DataFrame): DataFrame = {
    val dfResult = df
      .transform(removeNonPrintableChars(database))
    println("[INFO]: Data Cleaned Successfully.")
    dfResult
  }

  /**
   * Cleans bad data in all the StringType columns within a DataFrame
   *
   * @author Alberto Gaytan
   * @param df DataFrame to clean
   * @return Dataframe with non printable characters within.
   */
  def removeNonPrintableChars(database: String)(df: DataFrame): DataFrame = {
    val stringColumns = df.schema.fields
      .filter(item => item.dataType.equals(DataTypes.StringType))
      .map(i => i.name).toList

    df.columns
      .foldLeft(df) { (cleanedDF, colName) =>
        cleanedDF
          .withColumn(colName, regexNonPrintable(stringColumns, colName))
          .withColumn(colName, handleNullString(database, stringColumns, colName))
      }
  }

  /**
   *
   * @author Alberto Gaytan
   * @param stringColumns List of StringColumns to apply the regex
   * @param colName       ataFrame column
   * @return Dataframe Column where rows had 'null'  converted to 'null\s'
   */
  def handleNullString(database: String, stringColumns: List[String], colName: String): Column = {
    database match {
      case UtilFunctions.NETEZZA =>
        if (stringColumns.contains(colName))
          when(col(colName).equalTo("null"), lit("null ")).otherwise(col(colName))
        else {
          col(colName)
        }
      case _ => col(colName)
    }
  }

  /**
   * Regex to remove non printable characters inside a Dataframe column, notice that this tunction
   * only applies to StringType Columns.
   * \\p{Cf} .- other formatting
   * \\p{Cs} .- other characters above U+FFFF in Unicode
   *
   * @param stringColumns List of StringColumns to apply the regex
   * @param colName       DataFrame column
   * @return Dataframe Column with non printable chars removed from the string.
   */
  def regexNonPrintable(stringColumns: List[String], colName: String): Column = {
    if (stringColumns.contains(colName))
      regexp_replace(col(colName), "\\p{C}\\p{Cf}\\p{Cs}", "")
    else
      col(colName)
  }

  /**
   * Cast data all the TimestampType columns within a DataFrame removing the milliseconds.
   *
   * @author Alberto Gaytan
   * @param df DataFrame to clean
   * @return Dataframe with timestamps without millisecond
   */
  def truncateMilliseconds()(df: DataFrame): DataFrame = {
    val stringColumns = df.schema.fields
      .filter(item => item.dataType.equals(DataTypes.TimestampType))
      .map(i => i.name).toList

    df.columns
      .foldLeft(df) { (cleanedDF, colName) =>
        cleanedDF
          .withColumn(colName, castDateToRemoveMilliseconds(stringColumns, colName))
      }
  }

  /**
   * Function to remove milliseconds within a Timestamp column, only applies to Timestamp Columns.
   *
   * @param stringColumns List of StringColumns to apply the function
   * @param colName       DataFrame column
   * @return Dataframe Column with Timestamp without milliseconds.
   */
  def castDateToRemoveMilliseconds(stringColumns: List[String], colName: String): Column = {
    if (stringColumns.contains(colName))
      from_unixtime(unix_timestamp(col(colName), "yyyy-MM-dd HH:mm:ss.SSS"), "yyyy-MM-dd HH:mm:ss")
    else
      col(colName)
  }

  /**
   * Validates the DF source vs DF target in order to check that columns and data types are same.
   *
   * @author Alberto Gaytan
   * @param dfSource dataframe with new data to be inserted.
   * @param dfTarget dataframe with the current data in the database.
   * @return true in case everything is same or false in case any inconsistency.
   */
  def ValidateSchemaDF(dfSource: DataFrame, dfTarget: DataFrame): Boolean = {
    val requiredColNames = dfTarget.columns
    val requiredSchema = dfTarget.schema
    val step1 = validateMissingColumns(dfSource.columns.toSeq, requiredColNames)
    val step2 = validateMissingStructFields(dfSource.schema, requiredSchema)
    step1 && step2
  }

  /**
   * Validates two arrays to check if there are differences.
   *
   * @author Alberto Gaytan
   * @param source source array
   * @param target target array
   * @return true if there are differences otherwise false.
   */
  def validateMissingColumns(source: Seq[_], target: Seq[_]): Boolean = {
    val missingColumns = getDeltaElements(source, target)
    if (missingColumns.nonEmpty) {
      println(s"[WARN]: Delta COLUMNS: [$missingColumns] , either in source or target DataFrame.")
      false
    } else {
      println("[INFO]: COLUMNS in Sync between Source and Target.")
      true
    }
  }

  /**
   * Validates two arrays to check if there are difference.
   *
   * @author Alberto Gaytan
   * @param source source array
   * @param target target array
   * @return true if there are differences otherwise false.
   */
  def validateMissingStructFields(source: Seq[_], target: Seq[_]): Boolean = {
    val missingStructFields = getDeltaElements(source, target)
    if (missingStructFields.nonEmpty) {
      println(
        s"[WARN]: Delta STRUCTFIELDS: [${missingStructFields.replace("StructField", "")}]" +
          " either in source or target DataFrame.")
      false
    } else {
      println("[INFO]: STRUCTURE in Sync between Source and Target.")
      true
    }
  }

  /**
   * Returns Delta elements comparing two arrays
   *
   * @author Alberto Gaytan
   * @param source source Array
   * @param target Target Array
   * @return string with the difference between two arrays.
   */
  def getDeltaElements(source: Seq[_], target: Seq[_]): String = {
    if (target.length >= source.length)
      target.diff(source).mkString(", ")
    else
      source.diff(target).mkString(", ")
  }

  /**
   * Adds JOB_TS column if the column doesn't exist in the Dataframe
   *
   * @author Alberto Gaytan
   * @param df Dataframe with data to be inserted.
   * @return Dataframe with JOB_TS added.
   */
  def addJobTS()(df: DataFrame): DataFrame = {
    if (!df.columns.map(c => c.toUpperCase()).contains("JOB_TS"))
      df.withColumn("JOB_TS", current_timestamp().as("current_timestamp"))
    else
      df
  }

  /**
   * Compares DF source vs DF target, cast the columns same as DF target and put in sync the same columns than DF Target
   *
   * @author Alberto Gaytan
   * @param oldDF dataframe with the current data in the database (DF target)
   * @param newDF dataframe with new data to be inserted (DF source)
   * @return Dataframe with same datatypes and columns than DF target
   */
  def selectAndCastColumns(oldDF: DataFrame, newDF: DataFrame): DataFrame = {
    try {
      val columns = newDF.columns.toSet
      val df = newDF.select(oldDF.columns.map(c => {
        if (!columns.contains(c)) {
          lit(null).cast(oldDF.schema(c).dataType) as c
        } else {
          newDF(c).cast(oldDF.schema(c).dataType) as c
        }
      }): _*)
      println("[INFO]: Source columns casted Successfully.")
      df
    } catch {
      case unknown: Exception =>
        println("[ERROR]: Error while trying to cast Dataframe.\n Details:%s".format(unknown))
        null
    }
  }


  /**
   * Retrieves the general spark configuration to be used in all the applications.
   *
   * @author Alberto Gaytan
   * @return
   */
  def getSparkConf: SparkConf = {
    val sparkAppConf = new SparkConf()
    val props = new Properties()
    if (Try(Source.fromFile("./conf/spark.conf")).isSuccess) {
      props.load(Source.fromFile("./conf/spark.conf").bufferedReader())
      props.forEach((k, v) => sparkAppConf.set(k.toString, v.toString))
    }
    sparkAppConf
  }

  /**
   * Cleans cache tables and stop Spark services for the given SparkSession @session.
   *
   * @author Alberto Gaytan
   * @param session spark session to be finished.
   */
  def endJob(session: SparkSession): Unit = {
    if (!session.sparkContext.isStopped) {
      session.catalog.clearCache()
      session.stop()
      session.close()
    }
  }

  def getSecretOrElse(secretName: String, propertyName: String, default: String): (String, String) = {
    val path = "/etc/secrets/" + secretName + "/" + propertyName
    val source = Try(Source.fromFile(path))
    if (source.isSuccess) {
      val secret = source.get.mkString
      source.get.close()
      (propertyName, secret)
    } else {
      (propertyName, sys.env.getOrElse(propertyName, default))
    }
  }
}
