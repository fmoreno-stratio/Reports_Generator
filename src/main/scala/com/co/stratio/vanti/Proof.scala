package com.co.stratio.vanti

import java.util.regex.Pattern

import scala.io.{BufferedSource, Source}
import com.co.stratio.vanti.module.ERP
import com.sun.xml.internal.bind.v2.TODO
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object Proof {
  val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(Proof.getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  val properties: Map[String, String] = Map(
    //"pathFileReport" -> "/home/proyecto/Documentos/reportes_ERP_ISU/prueba.txt",
    "pathInputDirectoryReports" -> "/home/proyecto/Reports_Generator/reports/",//[ISU]_REPORT_PARQUET_DATE_OF_PROCESSS[04-08-2019T13_51_30].TXT", //por ahora tiene solo un reporte
    "pathOutputParquet" -> "/home/proyecto/Reports_Generator/out/officialParquet",
    "pathOutputCsv" -> "/home/proyecto/Reports_Generator/out/officialCsv",
    "module" -> "ERP",
    "extFile" -> ".TXT", // Gonna be a .txt
    "charset" -> "",
    "headersERP" -> "MODULO;TIPO_DE_REPORTE;RUTA_DE_REPORTE;FECHA_DE_GENERACION_DE_REPORTE;ARCHIVO_PROCESADO;NOMBRE_DE_TABLA_ASIGNADO_EN_LANDING_RAW_POR_ARCHIVO;NOMBRE_DE_TABLA_ASIGNADO_EN_LOS_PARAMETROS;CABECERA_ASIGNADA;CONTEO_CABECERA_ASIGNADO_ENVIADO_POR_EL_SISTEMA;CONTEO_CABECERA_POR_ARCHIVO;CABECERA_IDENTIFICADA_EN_EL_ARCHIVO;CABECERAS_IGUALES;NOMBRE_DE_DIRECTORIO;RUTA_EN_LANDING_RAW_ARCHIVO_SIN_TRANSFORMAR;TAMANO_DE_ARCHIVO_BYTES;VALIDACION_SHA;FORMATO_DE_ALMACENAMIENTO_DE_ARCHIVO_TRANSFORMADO;NOMBRE_TABLA;RUTA_EN_LANDINGRAW;TOTAL_COLUMNAS_PREPARACION_DE_MARCO_DE_DATOS;TOTAL_REGISTROS_PREPARACION_DE_MARCO_DE_DATOS;DIFERENCIA_TOTAL_COLUMNAS;DIFERENCIA_TOTAL_REGISTROS;TOTAL_COLUMNAS_OFICIAL;TOTAL_REGISTROS_OFICIAL;ESTADO_DEL_PROCESO;GEBERATION_DATE",
    "headersISU" -> ""
  )
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("dat - Local")
    .master("local[*]")
    .getOrCreate()

  val configuration: Configuration = new Configuration()
  val fs: FileSystem = FileSystem.get(configuration)

  //Properties
  val pathInputDirectoryReports = properties("pathInputDirectoryReports")
  val extFile = properties("extFile")
  val module = properties("module")
  val pathOutputParquet = properties("pathOutputParquet")
  val pathOutputCsv = properties("pathOutputCsv")

  var overrideParquet = false

  val columnsReport = properties("headersERP").split(";")
  val delimiterRow = "Ç"

  def main(args: Array[String]): Unit = {
    if(!fs.exists(new Path(s"${pathOutputParquet}/parquet"))){
      overrideParquet = true
    }
    initializer()
    //readFiles
  }

//  def main(args: Array[String]): Unit = {
//  val stream: FSDataInputStream = fs.open(new Path(pathInputDirectoryReports))
//  val source = Source.fromInputStream(stream)
//  val sc = sparkSession.sparkContext
//  val rdd = sc.parallelize(source.getLines.toSeq)
//    rdd.map(u =>{
//      println(u)
//    })
//  }

  private def initializer() = {
    val fileStatusListIterator: RemoteIterator[LocatedFileStatus] = fs.listFiles(new Path(pathInputDirectoryReports), true)
    while (fileStatusListIterator.hasNext) {
      try {
        val fileStatus: LocatedFileStatus = fileStatusListIterator.next
        val pathName = fileStatus.getPath.getName
        val pathFile = fileStatus.getPath

        if (pathName.endsWith(extFile)) {
          if(!overrideParquet){
            verificateIntoParquet(pathFile)
          }else{
            if(module.equals("ERP")) readFilesERP(pathFile)
            else readFilesISU(pathFile)
          }
        }
      }
      catch {
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
    val gl: Iterator[String] =source.getLines
    val rdd = sc.parallelize(gl.toSeq)
    var concat = ""

    val splitByChar = ":"
    val limitBlock = "status"
    val booleanCaster = "equalsHeaders"
    val t = getFields(rdd, pathFile)

    val rowsReport: RDD[Row] = rdd.repartition(1).map(line => {
      val l = line.split(splitByChar)
      if(Pattern.compile(s"\\b($booleanCaster)\\b").matcher(l(0)).find){
        val value = l(1).trim
        if(value.equals("true")) {
          println("its true  " + l(1))
          l(1) = "SI"
        }
        else{
          println("its false  " + l(1))
          l(1) = "NO"
        }
      }
      concat = s"$concat$delimiterRow${l(1)}"
      var strObj = Row()
      if (Pattern.compile(s"\\b($limitBlock)\\b").matcher(l(0)).find) {
        concat = s"$concat$t"
        strObj=splitInfoERP(concat.split(delimiterRow))
      }
      strObj
    }).filter(x=>{
      x.mkString.nonEmpty
    })
    rowsReport.foreach(x=>{
     println(x)
    })
    val dF= sparkSession.createDataFrame(rowsReport,generateSchema(columnsReport.toList))
    dF.show()

    dF
      .write
      .mode(SaveMode.Overwrite)
      .option("header", true)
      .option("quoteAll","true")
      .csv(pathOutputCsv)

    if(overrideParquet){
      dF.repartition(1).write.mode(SaveMode.Overwrite).parquet(pathOutputParquet)
      overrideParquet = false
    }
    else dF.repartition(1).write.mode(SaveMode.Append).parquet(pathOutputParquet)
    stream.close()
    source.close()
  }


    private def splitInfoERP(f: Array[String]) = {
      val obj = ERP()
      obj.module = f(23).toString.trim
      obj.reportType = f(24).toString.trim
      obj.reportPath = f(25).toString.trim
      obj.fileGeneratedDate = f(26).toString.trim
      obj.file = f(1).toString.trim
      obj.tableNameFromFile = f(2).toString.trim
      obj.tableNameFromJson = f(3).toString.trim
      obj.headersFromJson = f(4).toString.trim
      obj.countHeadersFromJson = f(5).toString.trim
      obj.countHeadersFromFile = f(6).toString.trim
      obj.headersFromFile = f(7).toString.trim
      obj.equalsHeaders = f(8).toString.trim
      obj.fileDirectory = f(11).toString.trim
      obj.filePath = f(12).toString.trim
      obj.fileSize = f(13).toString.trim
      obj.fileValidSha = f(14).toString.trim
      obj.fileColForSchema = f(15).toString.trim
      obj.fileTableName = f(16).toString.trim
      obj.fileColForPathTable = f(17).toString.trim
      obj.fileAntColForCountColumns = f(18).toString.trim
      obj.fileAntColForCountRows =  f(19).toString.trim
      obj.fileColForCountColumns =  f(20).toString.trim
      obj.fileColForCountRows =  f(21).toString.trim
      obj.difCountCol = f(27).toString.trim
      obj.difCountRow = f(28).toString.trim
      obj.status =  f(22).toString.trim
      obj.generationDate =  f(29).toString.trim

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
  private def verificateIntoParquet(pathFile: Path) = {
    val dfParquet = sparkSession.read.parquet(pathOutputParquet)

    readFilesERP(pathFile)
  }

  private def getFields(rdd: RDD[String], pathFile: Path): String = {

    /*
      module: String = "ERP",
  var reportType: String = "",
  var reportPath: String = "",
  var fileGeneratedDate: String = "",
  var difCountCol: String = "",
  var difCountRow: String = "",
  var generationDate: String = ""
     */

    var reportType = ""
    val reportPath = pathFile
    val fileGeneratedDate ="cualquier cosa" //regrex
    var difCountCol = "Proof"
    var difCountRow = "Proof2"
    val generationDate = "Generate Date"

    val matchType = "parquet"
    val reportTypeMessage = "Generación de Parquet"
    val dR = "Ç"
    val booleanCaster = "equalsHeaders"

    rdd.repartition(1).map(line =>{
      val l = line.split(":")
      if(Pattern.compile(s"\\b($matchType)\\b").matcher(l(1)).find){
        reportType = reportTypeMessage
      }
      if(Pattern.compile(s"\\b($booleanCaster)\\b").matcher(l(1)).find){
        val value = l(1)
        if(value == "false") l(1) = "NO"
        else l(1) = "SI"
      }
    })

    val t = s"$dR$module$dR$matchType$dR$reportPath$dR$fileGeneratedDate$dR$difCountCol$dR$difCountRow$dR$generationDate"
    t
  }

  private def generateSchema(lstFields: List[String]): StructType = {
    val schema: StructType.type = StructType
    var seq: List[StructField] = List()
    for (o <- lstFields.reverse) {
      seq = StructField(name = o, dataType = StringType, nullable = true) :: seq
    }
    val stType: StructType = schema.apply(seq)
    stType
  }

}
