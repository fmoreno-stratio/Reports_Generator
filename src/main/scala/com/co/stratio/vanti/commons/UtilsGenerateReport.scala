package com.co.stratio.vanti.commons

import java.io.{PrintWriter, StringWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.Logger

import scala.reflect.io.Path
import scala.util.Try

object UtilsGenerateReport {
  val tab = "\t"
  val lineSeparator: String = System.getProperty("line.separator")
  val delimiter: String = ";"

  var message: String = s"[VANTI]."

  def getDateAsString: String = {
    val dateFormat = new SimpleDateFormat("dd-MM-yyyy'T'HH:mm:ss")
    dateFormat.format(Calendar.getInstance().getTime)
  }

  def deleteDirectory(pathDirectory: String): Unit = {
    val path: Path = Path(pathDirectory)
    Try(path.deleteRecursively())
  }

  def printError(log: Logger, fail: Throwable, nameClass: String, method: String): Unit = {
    val sw = new StringWriter
    fail.printStackTrace(new PrintWriter(sw))
    val trace = sw.toString
    log.error(s"Failed $message[$nameClass].[$method] Reason: $fail StackTrace: $trace")
  }

  def prepareDataFrame(session: SparkSession, schema: StructType, charset: String, fileName: String, delimiter: String): DataFrame = {
    val df = session.read
      .option("charset", charset)
      .option("delimiter", delimiter)
      .schema(schema)
      .csv(fileName)
    df
  }

  def obtainHeaders(fieldsByTable: List[(String, String)], table: String): List[String] = {
    val filterTable: Seq[(String, String)] = fieldsByTable.filter(_._1.equals(table))
    val lstFields: List[String] = filterTable.map(x => x._2).toList.reverse
    lstFields
  }

  def generateSchema(lstFields: List[String]): StructType = {
    val schema: StructType.type = StructType
    var seq: List[StructField] = List()
    for (o <- lstFields.reverse) {
      seq = StructField(name = o, dataType = StringType, nullable = true) :: seq
    }
    val stType: StructType = schema.apply(seq)
    stType
  }
}
