package org.arg.common.utilities

import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime

import java.text.SimpleDateFormat

class JobListener extends SparkListener {
  var recordsWritten = 0L
  var recordsRead = 0L
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart) {
    println("[INFO]: JOB START: " + dateFormat.format(new DateTime(applicationStart.time).toDate))
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
    println("[INFO]: JOB END: " + dateFormat.format(new DateTime(applicationEnd.time).toDate))
  }

  def getOutputMetrics(session: SparkSession): Unit = {
    session.sparkContext.addSparkListener(new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
        val metrics = taskEnd.taskMetrics
        if (metrics.inputMetrics != null)
          recordsRead += metrics.inputMetrics.recordsRead
        if (metrics.outputMetrics != null)
          recordsWritten += metrics.outputMetrics.recordsWritten
      }
    })
  }
}
