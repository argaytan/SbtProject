package org.arg.common.utilities

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.io.File

/**
 * Class that contains only CSV functions.
 *
 * @author Alberto Gaytan
 * @todo Add csv functions in case the desired functionality isn't created yet.
 */
class CSVFunctions {
  val outputPath: String = System.getenv("PARQUETS_OUTPUT")

  /**
   * Writes data from @df to csv file with filename @name inside the path specified in the
   * env variable csvS_OUTPUT
   * @author Alberto Gaytan
   * @param df   Dataframe to write in the csv file.
   * @param name csv file name.
   * @example writecsv(df_clobCombine, "Report_AI")
   * @note Don't specify ".csv" at the end of @name
   * @return Boolean indicator, if the process was success=true or failed=false
   */
  def writeCSV(df: DataFrame, name: String): String = {
    val csvDir = validateOrCreateNewName(name)
    val csvName = csvDir.split("/").last
    try {
      if (df.rdd.getNumPartitions > 1)
        df.coalesce(1).write.mode(SaveMode.Overwrite).csv(csvDir)
      else
        df.write.mode(SaveMode.Overwrite).csv(csvDir)
      println("[INFO]: %s csv file created successfully.".format(csvName))
      csvDir
    } catch {
      case unknown: Exception =>
        throw new Exception("[ERROR]: Error to create csv: %s.\n Error description:%s".format(csvName, unknown))
    }
  }

  /**
   *  Validates the name provided as csv , if the name exist will create a unique file name othercase will create
   *  the csv with the name provided.Additional if the output dir doesn't exist will create the folder.
   * @author Alberto Gaytan
   * @param name csv name
   * @return
   */
  def validateOrCreateNewName(name: String): String = {
    val uniqueID = java.util.UUID.randomUUID.toString
    val folder = new File(outputPath)
    val path = new File(outputPath.concat(name).concat(".csv"))
    try {
      if (!folder.exists()) folder.mkdir()
      if (path.exists())
        outputPath.concat(name).concat("-").concat(uniqueID).concat(".csv")
      else
        outputPath.concat(name).concat(".csv")
    } catch {
      case unknown: Exception =>
        throw new Exception("[ERROR]: Error creating csv path.\n Error description:%s".format(unknown))
    }
  }

  /**
   * Reads data with csv filename @name inside "./output/". and return a DataFrame with the data.
   *
   * @author Alberto Gaytan
   * @param session sparkSession created in your job.
   * @param path    full path csv location.
   * @example
   * @return DataFrame with data specified in csv file @name.
   */
  def readcsv(session: SparkSession, path: String): DataFrame = {
    val name = path.split("/").last
    try {
      // Write file to csv
      val df = session.read.csv(path)
      println("[INFO]: %s csv file readed successfully.".format(name))
      df
    } catch {
      case unknown: Exception =>
        println("[ERROR]: Error reading csv %s.\n Error description:%s".format(name, unknown))
        null
    }
  }

}
