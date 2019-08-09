package com.co.stratio.vanti

import java.util.regex.Pattern

import com.co.stratio.vanti.commons.UtilsGenerateReport

import scala.io.{BufferedSource, Source}
import com.co.stratio.vanti.module.ERP
import com.sun.xml.internal.bind.v2.TODO
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

object Proof {
  val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(Proof.getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  val properties: Map[String, String] = Map(
    //"pathFileReport" -> "/home/proyecto/Documentos/reportes_ERP_ISU/prueba.txt",
    "pathInputDirectoryReports" -> "C:\\SparkScala\\DataToFormatColumnar\\DataToFormatColumnar\\landingRaw\\ERP\\reports\\", //[ISU]_REPORT_PARQUET_DATE_OF_PROCESSS[04-08-2019T13_51_30].TXT", //por ahora tiene solo un reporte
    "pathOutputParquet" -> "out/proyecto/Reports_Generator/out/officialParquet",
    "pathOutputCsv" -> "out/proyecto/Reports_Generator/out/officialCsv",
    "module" -> "ERP",
    "extFile" -> ".TXT", // Gonna be a .txt
    "charset" -> "",
    "headersERP" -> "MODULO;TIPO_DE_REPORTE;RUTA_DE_REPORTE;FECHA_DE_GENERACION_DE_REPORTE;ARCHIVO_PROCESADO;NOMBRE_DE_TABLA_ASIGNADO_EN_LANDING_RAW_POR_ARCHIVO;NOMBRE_DE_TABLA_ASIGNADO_EN_LOS_PARAMETROS;CABECERA_ASIGNADA;CONTEO_CABECERA_ASIGNADO_ENVIADO_POR_EL_SISTEMA;CONTEO_CABECERA_POR_ARCHIVO;CABECERA_IDENTIFICADA_EN_EL_ARCHIVO;CABECERAS_IGUALES;NOMBRE_DE_DIRECTORIO;RUTA_EN_LANDING_RAW_ARCHIVO_SIN_TRANSFORMAR;TAMANO_DE_ARCHIVO_BYTES;VALIDACION_SHA;FORMATO_DE_ALMACENAMIENTO_DE_ARCHIVO_TRANSFORMADO;NOMBRE_TABLA;RUTA_EN_LANDINGRAW;TOTAL_COLUMNAS_PREPARACION_DE_MARCO_DE_DATOS;TOTAL_REGISTROS_PREPARACION_DE_MARCO_DE_DATOS;TOTAL_COLUMNAS_OFICIAL;TOTAL_REGISTROS_OFICIAL;DIFERENCIA_TOTAL_COLUMNAS;DIFERENCIA_TOTAL_REGISTROS;ESTADO_DEL_PROCESO;GEBERATION_DATE",
    "headersISU" -> ""
  )
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("dat - Local")
    .master("local[*]")
    .getOrCreate()

  val configuration: Configuration = new Configuration()
  val fs: FileSystem = FileSystem.get(configuration)

  //Properties
  val pathInputDirectoryReports: String = properties("pathInputDirectoryReports")
  val extFile: String = properties("extFile")
  val module: String = properties("module")
  val pathOutputParquet: String = properties("pathOutputParquet")
  val pathOutputCsv: String = properties("pathOutputCsv")

  var overrideParquet: Boolean = !fs.exists(new Path(s"${pathOutputParquet}/parquet"))

  val columnsReport: Array[String] = properties("headersERP").split(";")
  val delimiterRow = "Ç"

  def main(args: Array[String]): Unit = {
    initializer()
  }

  private def initializer(): Unit = {
    val fileStatusListIterator: RemoteIterator[LocatedFileStatus] = fs.listFiles(new Path(pathInputDirectoryReports), true)
    while (fileStatusListIterator.hasNext) {
      try {
        val fileStatus: LocatedFileStatus = fileStatusListIterator.next
        val pathName = fileStatus.getPath.getName
        val pathFile = fileStatus.getPath

        if (pathName.endsWith(extFile)) {
          if (!overrideParquet) {
            verificateIntoParquet(pathFile)
          } else if (module.equals("ERP")) {
            readFilesERP(pathFile)
          } else if (module.equals("ISU")) {
            readFilesISU(pathFile)
          }

        }
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

  /*
  TODO
    Ask what type of module is.
   */


  private def readFilesERP(pathFile: Path): Unit = {

    val stream: FSDataInputStream = fs.open(pathFile)
    val source: BufferedSource = Source.fromInputStream(stream)
    val sc = sparkSession.sparkContext
    val gl: Iterator[String] = source.getLines
    val rdd = sc.parallelize(gl.toSeq)
    var concat = ""

    val splitByChar = ":"
    val limitBlock = "status"

    val t = getFields(rdd, pathFile)

    val rowsReport: RDD[Row] = rdd.repartition(1).map(line => {
      val l = line.split(splitByChar)
      concat = s"$concat$delimiterRow${l(1)}"
      var strObj = Row()
      if (Pattern.compile(s"\\b($limitBlock)\\b").matcher(l(0)).find) {
        concat = s"$concat$t"
        strObj = splitInfoERP(concat.split(delimiterRow))
        concat = ""
      }
      strObj
    }).filter(x => {
      x.mkString.nonEmpty
    })

    val dF = sparkSession.createDataFrame(rowsReport, UtilsGenerateReport.generateSchema(columnsReport.toList))
    dF.show()

    dF.write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("quoteAll", "true")
      .csv(pathOutputCsv)

    if (overrideParquet) {
      dF.repartition(1).write.mode(SaveMode.Overwrite).parquet(pathOutputParquet)
      overrideParquet = false
    }
    else dF.repartition(1).write.mode(SaveMode.Append).parquet(pathOutputParquet)
    stream.close()
    source.close()
  }


  private def splitInfoERP(f: Array[String]) = {
    val obj = ERP()
    obj.module = f(23).trim
    obj.reportType = s"Generación de ${f(24).trim}"
    obj.reportPath = f(25).trim
    val date = raw"(\d{2})-(\d{2})-(\d{4})T(\d{2})_(\d{2})_(\d{2})".r
    obj.fileGeneratedDate = date.findFirstIn(obj.reportPath).get
    obj.file = f(1).trim
    obj.tableNameFromFile = f(2).trim
    obj.tableNameFromJson = f(3).trim
    obj.headersFromJson = f(4).trim
    obj.countHeadersFromJson = f(5).trim
    obj.countHeadersFromFile = f(6).trim
    obj.headersFromFile = f(7).trim
    obj.equalsHeaders = if (f(8).trim.equals("true")) "SI" else "NO"
    obj.fileDirectory = f(11).trim
    obj.filePath = f(12).trim
    obj.fileSize = f(13).trim
    obj.fileValidSha = f(14).trim
    obj.fileColForSchema = f(15).trim
    obj.fileTableName = f(16).trim
    obj.fileColForPathTable = f(17).trim
    obj.fileAntColForCountColumns = f(18).trim
    obj.fileAntColForCountRows = f(19).trim
    obj.fileColForCountColumns = f(20).trim
    obj.fileColForCountRows = f(21).trim
    obj.difCountCol = Math.abs(stringToInt(obj.fileAntColForCountColumns) - stringToInt(obj.fileColForCountColumns)).toString
    obj.difCountRow = Math.abs(stringToInt(obj.fileAntColForCountRows) - stringToInt(obj.fileColForCountRows)).toString
    obj.status = f(22).trim
    val dateTime = UtilsGenerateReport.getDateAsString
    obj.generationDate = s"${dateTime.replaceAll(":", "_")}"

    Row(obj.module,
      obj.reportType,
      obj.reportPath,
      obj.fileGeneratedDate,
      obj.file,
      obj.tableNameFromFile,
      obj.tableNameFromJson,
      obj.headersFromJson,
      obj.countHeadersFromJson,
      obj.countHeadersFromFile,
      obj.headersFromFile,
      obj.equalsHeaders,
      obj.fileDirectory,
      obj.filePath,
      obj.fileSize,
      obj.fileValidSha,
      obj.fileColForSchema,
      obj.fileTableName,
      obj.fileColForPathTable,
      obj.fileAntColForCountColumns,
      obj.fileAntColForCountRows,
      obj.fileColForCountColumns,
      obj.fileColForCountRows,
      obj.difCountCol,
      obj.difCountRow,
      obj.status,
      obj.generationDate)
  }

  private def stringToInt(s: String): Int = {
    Try(s.trim.toInt) match {
      case Success(success) => success
      case Failure(_) => 0
    }
  }

  /*
  TODO
    Do the process to ISU
   */
  private def readFilesISU(pathFile: Path): Unit = {

  }

  private def splitInfoISU(f: Array[String]) = {

  }

  /*
  TODO
   verify if the report was already processed
    if was already processed report it -- if not, go to readFiles method, cosidering module
   */
  private def verificateIntoParquet(pathFile: Path): Unit = {
    val dfParquet = sparkSession.read.parquet(pathOutputParquet)
    readFilesERP(pathFile)
  }

  private def getFields(rdd: RDD[String], pathFile: Path): String = {
    val reportPath = pathFile
    val matchType = "parquet"
    val dR = "Ç"
    s"$dR$module$dR$matchType$dR$reportPath"
  }


}
