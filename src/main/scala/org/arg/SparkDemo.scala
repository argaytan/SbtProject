package org.arg

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.util.Properties
import scala.io.Source
import scala.util.Try

object SparkDemo {
  def main(args: Array[String]): Unit = {
    val logger: Logger = Logger.getRootLogger

    logger.warn("Starting SparkDemo Application")
    val spark: SparkSession = SparkSession.builder()
      .appName("SparkDemo")
      .config(getSparkConf)
      .getOrCreate()

    logger.warn("getting file lines...")
    val lines = spark.sparkContext.textFile("/Users/agaytan/Documents/EDUCATION/SPARK/IntroSpark/book.txt")

    logger.warn("split lines by emptyspace...")
    val words = lines.flatMap(line => line.split(' '))
    logger.warn("map words...")
    val wordsKVRdd = words.map(x => (x, 1))
    logger.warn("counting words...")
    val count = wordsKVRdd
      .reduceByKey((x, y) => x + y).map(x => (x._2, x._1))
      .sortByKey(ascending = false)
      .map(x => (x._2, x._1))
      .take(10)
    logger.warn("print count list...")
    count.foreach(println)

    logger.warn("------ RDD ------")
    val data = Seq(Row(Row("James ", "", "Smith"), "36636", "M", 3000),
      Row(Row("Michael ", "Rose", ""), "40288", "M", 4000),
      Row(Row("Robert ", "", "Williams"), "42114", "M", 4000),
      Row(Row("Maria ", "Anne", "Jones"), "39192", "F", 4000),
      Row(Row("Jen", "Mary", "Brown"), "", "F", -1)
    )

    logger.warn("------ Create schema for Dataframe ------")
    val schema = new StructType()
      .add("name", new StructType()
        .add("firstname", StringType)
        .add("middlename", StringType)
        .add("lastname", StringType))
      .add("id", StringType)
      .add("gender", StringType)
      .add("salary", IntegerType)

    logger.warn("------ Dataframe from RDD with schema and data ------")
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    df.printSchema()
    df.show(false)

    logger.warn("--- Retrieving data from Struct column ---")
    // collect() does not return a Dataframe instead, it returns data in
    // an array to your driver. once the data is collected in an array,
    // you can use scala language for further processing.
    //
    // Usually, collect() is used to retrieve the action output when you have
    // very small result set and calling collect() on an RDD/DataFrame with a
    // bigger result set causes out of memory as it returns the entire dataset
    // (from all workers) to the driver hence we should avoid calling collect()
    // on a larger dataset.
    val colList = df.collectAsList()
    println(colList)
    val colData = df.collect()
    colData.foreach(row => {
      val salary = row.getInt(3)
      val fullName: Row = row.getStruct(0) //Index starts from zero
      //      val firstName = fullName.getString(0) //In struct row, again index starts from zero
      //      val middleName = fullName.get(1).toString
      val firstName = fullName.getAs[String]("firstname").trim
      val middleName = fullName.getAs[String]("middlename").trim
      val lastName = fullName.getAs[String]("lastname").trim
      println(firstName + " " + middleName + " " + lastName + ", " + salary)
    })


    // return certain elements of a DataFrame, you should call select() first
    // and then call the apply() method on the resulting DataFrame.
    //
    // select() method on an RDD/DataFrame returns a new DataFrame that holds
    // the columns that are selected whereas collect() returns the entire data set.
    //
    // select() is a transformation function whereas collect() is an action.
    logger.warn("--- Retrieving data from Struct column ---")
    val df2 = df.select("name.firstname", "name.middlename","name.lastname", "salary")
    df2.show(false)




    logger.warn("delay 10 secs...")
    Thread.sleep(10000)
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

}


