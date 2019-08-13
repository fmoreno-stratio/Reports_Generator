package com.co.stratio.vanti

import java.util.regex.Pattern

import com.co.stratio.vanti.commons.UtilsGenerateReport
import com.co.stratio.vanti.module.Module
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import scala.io.{BufferedSource, Source}
import scala.util.{Failure, Success, Try}

object Proof {
  val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(Proof.getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  val properties: Map[String, String] = Map(
    "pathDirectory" -> "/home/proyecto/Documentos/[ISU]_REPORT_PARQUET_DATE_OF_PROCESS[04-08-2019T13_52_37].TXT", //"C:\\SparkScala\\DataToFormatColumnar\\DataToFormatColumnar\\landingRaw\\ERP\\reports\\",
    "pathOutputParquet" -> "out/officialParquet",
    "pathOutputCsv" -> "out/officialCsv",
    "module" -> "ISU",
    "extFile" -> ".TXT",
    "charset" -> "",
    "headers" -> "MODULO;TIPO_DE_REPORTE;RUTA_DE_REPORTE;FECHA_DE_GENERACION_DE_REPORTE;INFORMACION_ZIP;NOMBRE_ZIP;DIRECTORIO_ZIP;RUTA_ZIP;TAMANO_ZIP;ARCHIVO_PROCESADO;NOMBRE_DE_TABLA_ASIGNADO_EN_LANDING_RAW_POR_ARCHIVO;NOMBRE_DE_TABLA_ASIGNADO_EN_LOS_PARAMETROS;CABECERA_ASIGNADA;CONTEO_CABECERA_ASIGNADO_ENVIADO_POR_EL_SISTEMA;CONTEO_CABECERA_POR_ARCHIVO;CABECERA_IDENTIFICADA_EN_EL_ARCHIVO;CABECERAS_IGUALES;NOMBRE_DE_DIRECTORIO;RUTA_EN_LANDING_RAW_ARCHIVO_SIN_TRANSFORMAR;TAMANO_DE_ARCHIVO_BYTES;VALIDACION_SHA;FORMATO_DE_ALMACENAMIENTO_DE_ARCHIVO_TRANSFORMADO;NOMBRE_TABLA;RUTA_EN_LANDINGRAW;TOTAL_COLUMNAS_PREPARACION_DE_MARCO_DE_DATOS;TOTAL_REGISTROS_PREPARACION_DE_MARCO_DE_DATOS;TOTAL_COLUMNAS_OFICIAL;TOTAL_REGISTROS_OFICIAL;DIFERENCIA_TOTAL_COLUMNAS;DIFERENCIA_TOTAL_REGISTROS;ESTADO_DEL_PROCESO;GEBERATION_DATE"
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

  val columnsReport: Array[String] = properties("headers").split(";")
  val delimiterRow = "Ç"

  def main(args: Array[String]): Unit = {
    initializer()
  }

  private def initializer(): Unit = {
    val fileStatusListIterator: RemoteIterator[LocatedFileStatus] = fs.listFiles(new Path(pathDirectory), true)
    while (fileStatusListIterator.hasNext) {
      try {
        val fileStatus: LocatedFileStatus = fileStatusListIterator.next
        val pathName = fileStatus.getPath.getName
        val pathFile = fileStatus.getPath

        if (pathName.endsWith(extFile) && !fs.exists(new Path(s"$pathDirectory.OFRCORRECT"))) {
          val stream: FSDataInputStream = fs.open(pathFile)
          val source: BufferedSource = Source.fromInputStream(stream)
          readFiles(pathFile, source) match {
            case Success(_) => {
              val fileOut: FSDataOutputStream = fs.create(new Path(s"$pathDirectory.OFRCORRECT"))
              fileOut.write(s"[OK] - Creación de parquet exitosa para el informe $pathFile".getBytes())
              fileOut.close()
            }
            case Failure(fail) => {
              fail.printStackTrace()
              val fileOut: FSDataOutputStream = fs.create(new Path(s"$pathDirectory.OFRINCORRECT"))
              fileOut.write(s"[NOT OK] - Creación de parquet no exitosa para el informe $pathFile".getBytes())
              fileOut.close()
            }
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
        if (module.equals("ERP")) {
          strObj = splitInfoERP(concat.split(delimiterRow))
        } else if(module.equals("ISU")) {
          strObj = splitInfoISU(concat.split(delimiterRow))
        } else if (module.equals("SAT")){
          // TODO: Do to Satelites' module case
        }
        concat = ""
      }
      strObj
    }).filter(x => {
      x.mkString.nonEmpty
    })

    val dF = sparkSession.createDataFrame(rowsReport, UtilsGenerateReport.generateSchema(columnsReport.toList)).distinct().orderBy(col("NOMBRE_DE_TABLA_ASIGNADO_EN_LANDING_RAW_POR_ARCHIVO"), col("DIFERENCIA_TOTAL_REGISTROS").asc)
    dF.show()

    dF.repartition(1).write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("quoteAll", "true")
      .csv(pathOutputCsv)

    dF.repartition(1).write.mode(SaveMode.Append).parquet(pathOutputParquet)
  }

  private def splitInfoERP(f: Array[String]) = {
    val obj = Module()
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
    obj.generationDate = s"${dateTime}"

    Row(obj.module,
      obj.reportType,
      obj.reportPath,
      obj.fileGeneratedDate,
      obj.zipInformation,
      obj.zipName,
      obj.zipDirectory,
      obj.zipPath,
      obj.zipSize,
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

  private def splitInfoISU(f: Array[String]) = {
    val obj = Module()
    obj.module = f(28).trim
    obj.reportType = s"Generación de ${f(29).trim}"
    obj.reportPath = f(30).trim
    val date = raw"(\d{2})-(\d{2})-(\d{4})T(\d{2})_(\d{2})_(\d{2})".r
    obj.fileGeneratedDate = date.findFirstIn(obj.reportPath).get
    obj.zipInformation = f(1).trim
    obj.zipName = f(2).trim
    obj.zipDirectory = f(3).trim
    obj.zipPath = f(4).trim
    obj.zipSize = f(5).trim
    obj.file = f(6).trim
    obj.tableNameFromFile = f(7).trim
    obj.tableNameFromJson = f(8).trim
    obj.headersFromJson = f(9).trim
    obj.countHeadersFromJson = f(10).trim
    obj.countHeadersFromFile = f(11).trim
    obj.headersFromFile = f(12).trim
    obj.equalsHeaders = if (f(13).trim.equals("true")) "SI" else "NO"
    obj.fileDirectory = f(16).trim
    obj.filePath = f(17).trim
    obj.fileSize = f(18).trim
    obj.fileValidSha = f(19).trim
    obj.fileColForSchema = f(20).trim
    obj.fileTableName = f(21).trim
    obj.fileColForPathTable = f(22).trim
    obj.fileAntColForCountColumns = f(23).trim
    obj.fileAntColForCountRows = f(24).trim
    obj.fileColForCountColumns = f(25).trim
    obj.fileColForCountRows = f(26).trim
    obj.difCountCol = Math.abs(stringToInt(obj.fileAntColForCountColumns) - stringToInt(obj.fileColForCountColumns)).toString
    obj.difCountRow = Math.abs(stringToInt(obj.fileAntColForCountRows) - stringToInt(obj.fileColForCountRows)).toString
    obj.status = f(27).trim
    val dateTime = UtilsGenerateReport.getDateAsString
    obj.generationDate = s"${dateTime}"

    Row(obj.module,
      obj.reportType,
      obj.reportPath,
      obj.fileGeneratedDate,
      obj.zipInformation,
      obj.zipName,
      obj.zipDirectory,
      obj.zipPath,
      obj.zipSize,
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

  private def getFields(rdd: RDD[String], pathFile: Path): String = {
    val reportPath = pathFile
    val matchType = "parquet"
    val dR = "Ç"
    s"$dR$module$dR$matchType$dR$reportPath"
  }

  private def stringToInt(s: String): Int = {
    Try(s.trim.toInt) match {
      case Success(success) => success
      case Failure(_) => 0
    }
  }

}
