package com.co.stratio.vanti

import java.util.regex.Pattern

import com.co.stratio.vanti.commons.UtilsGenerateReport
import com.co.stratio.vanti.module.Module
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.io.{BufferedSource, Source}
import scala.util.{Failure, Success, Try}

object ProofLL {
  val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(Proof.getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  val properties: Map[String, String] = Map(
    //    "pathDirectory" -> "/home/fernanda/Escritorio/Prueba", //"C:\\SparkScala\\DataToFormatColumnar\\DataToFormatColumnar\\landingRaw\\ERP\\reports\\",
    "pathDirectory" -> "C:\\Users\\luisa\\luis\\Stratio\\VANTI\\info_paths_hdfs\\ISU\\CI\\reports", //"C:\\SparkScala\\DataToFormatColumnar\\DataToFormatColumnar\\landingRaw\\ERP\\reports\\",
    "pathOutputParquet" -> "out/officialParquet",
    "pathOutputCsv" -> "out/officialCsv",
    "module" -> "ISU",
    "extFile" -> ".TXT",
    "charset" -> "",
    "headers" -> "_c0,_c1,_c2,_c3,_c4,_c5,_c6,_c7,_c8,_c9,_c10,_c11,_c12,_c13,_c14,_c15,_c16,_c17,_c18,_c19,_c20,_c21,_c22,_c23,_c24,_c25,_c26,_c27"
  )
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("dat - Local")
    .master("local[*]")
    .getOrCreate()

  val configuration: Configuration = new Configuration()
  val fs: FileSystem = FileSystem.get(configuration)

  /**
   * Properties
   */
  val pathDirectory: String = properties("pathDirectory")
  val extFile: String = properties("extFile")
  val module: String = properties("module")
  val pathOutputParquet: String = properties("pathOutputParquet")
  val pathOutputCsv: String = properties("pathOutputCsv")
  val columnsReport: Array[String] = properties("headers").split(",")

  def main(args: Array[String]): Unit = {
    initializer()
    sparkSession.read.parquet("C:\\SparkScala\\Reports_Generator\\out\\officialParquet")
      .orderBy(col("FC_RUTA_TABLA").desc, col("FC_CANT_REG_CR").cast(IntegerType).asc)
      .repartition(1).write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("quoteAll", "true")
      .csv(pathOutputCsv)
  }

  private def initializer(): Unit = {
    val fileStatusListIterator: RemoteIterator[LocatedFileStatus] = fs.listFiles(new Path(pathDirectory), true)
    while (fileStatusListIterator.hasNext) {
      try {
        val fileStatus: LocatedFileStatus = fileStatusListIterator.next
        val pathName = fileStatus.getPath.getName
        val pathFile = fileStatus.getPath
        val pathDir = fileStatus.getPath.getParent.getName

        if (pathName.endsWith(extFile) && !fs.exists(new Path(s"$pathDirectory/$pathDir/$pathName.OFRCORRECT"))) {
          val stream: FSDataInputStream = fs.open(pathFile)
          val source: BufferedSource = Source.fromInputStream(stream)
          readFiles(pathFile, source) match {
            case Success(_) =>
              val fileOut: FSDataOutputStream = fs.create(new Path(s"$pathDirectory/$pathDir/$pathName.OFRCORRECT"), true)
              fileOut.write(s"[OK] - Creación de parquet exitosa para el informe $pathFile".getBytes())
              fileOut.close()
            case Failure(fail) =>
              fail.printStackTrace()
              val fileOut: FSDataOutputStream = fs.create(new Path(s"$pathDirectory/$pathDir/$pathName.OFRINCORRECT"), true)
              fileOut.write(s"[NOT OK] - Creación de parquet no exitosa para el informe $pathFile".getBytes())
              fileOut.close()

          }
          source.close()
          IOUtils.closeStream(stream)
        }
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

  private def readFiles(pathFile: Path, source: Source) = Try {

    val sc = sparkSession.sparkContext
    val gl: Iterator[String] = source.getLines
    val rdd: RDD[String] = sc.parallelize(gl.toSeq)
    val t = getFields(rdd, pathFile)
    val report = s"$t*Þ*${gl.mkString("*Þ*")}"
    val clean = report.filterNot(c => c.equals('├') || c.equals('└'))
    val lst = clean.split("\\*Þ\\*")
    val rowsReport = lst.map(x => {
      x.replaceAll("^\\s+", "").replaceAll("^([A-Za-z:]*\\s)", "").trim
    })
    val row = Row.fromSeq(rowsReport)
    val rddRowsReport = sc.parallelize(List(row))
    val columnsHom = getHomColumns

    val dF = sparkSession.createDataFrame(rddRowsReport, UtilsGenerateReport.generateSchema(columnsHom.toList)).distinct()
      .withColumn("F_PROC", current_date())
      .withColumn("FH_PROC", current_timestamp())
    dF.repartition(1).write.mode(SaveMode.Append).parquet(pathOutputParquet)
  }

  private def getHomColumns: Array[String] = {
    val columnsHom = columnsReport.map({
      case "_c0" => "MODULO";
      case "_c1" => "TIPO";
      case "_c2" => "RUTA";
      case "_c3" => "NOM_ZIP";
      case "_c4" => "DIR_ZIP";
      case "_c5" => "RUTA_ZIP";
      case "_c6" => "PESO_ZIP";
      case "_c7" => "NOM_ARCHIVO";
      case "_c8" => "NOM_TABLA";
      case "_c9" => "CAB_ENVIADA";
      case "_c10" => "CAN_DELIM_ENVIADO";
      case "_c11" => "CAB_ARCHIVO";
      case "_c12" => "CAN_DELIM_ARCHIVO";
      case "_c13" => "CAB_IGUALES";
      case "_c14" => "DET_ARCHIVO";
      case "_c15" => "DET_NOM_ARCHIVO";
      case "_c16" => "DET_DIR_ARCHIVO";
      case "_c17" => "DET_RUT_ARCHIVO";
      case "_c18" => "DET_PESO_ARCHIVO";
      case "_c19" => "DET_SHA_ARCHIVO";
      case "_c20" => "FC_NUEVO";
      case "_c21" => "FC_TABLA";
      case "_c22" => "FC_RUTA_TABLA";
      case "_c23" => "FC_CANT_COL_INIT";
      case "_c24" => "FC_CANT_REG_INIT";
      case "_c25" => "FC_CANT_COL_CR";
      case "_c26" => "FC_CANT_REG_CR";
      case "_c27" => "ESTADO";
      case x => x
    })
    columnsHom
  }

  private def getFields(rdd: RDD[String], pathFile: Path): String = {
    val reportPath = pathFile.toString
    val matchType = "parquet"
    val dR = "*Þ*"
    s"$module$dR$matchType$dR$reportPath"
  }


}
