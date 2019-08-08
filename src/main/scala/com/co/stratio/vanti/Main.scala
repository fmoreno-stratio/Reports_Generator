package com.co.stratio.vanti

import java.util.regex.Pattern

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.io.Source

object Main {
  val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(Main.getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  val properties: Map[String, String] = Map(
    //"pathFileReport" -> "/home/proyecto/Documentos/reportes_ERP_ISU/prueba.txt",
    "pathDirectory" -> "/home/proyecto/Documentos/reportes_ERP_ISU/[ISU]_REPORT_PARQUET_DATE_OF_PROCESSS[04-08-2019T13_51_30].TXT", //por ahora tiene solo un reporte
    "pathOutputParquet" -> "",
    "pathOutputCsv" -> "",
    "module" -> "",
    "extFile" -> "",
    "charset" -> "",
    "headerERP" -> "",
    "headerISU" -> ""
  )
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("dat - Local")
    .master("local[*]")
    .getOrCreate()

  val sc = sparkSession.sparkContext
  val configuration: Configuration = new Configuration()
  val fs: FileSystem = FileSystem.get(configuration)

  val pathDirectory = properties("pathDirectory")
  val module = properties("module")

  def main(args: Array[String]): Unit = {
    val dfParquet = sparkSession.read.parquet("/home/proyecto/Reports_Generator/out/officialParquet")
    dfParquet.show()
  }

  val columnsReport = ("ARCHIVO_PROCESADO;" +
    "NOMBRE_DE_TABLA_ASIGNADO_EN_LANDING_RAW_POR_ARCHIVO;" +
    "NOMBRE_DE_TABLA_ASIGNADO_EN_LOS_PARAMETROS;" +
    "CABECERA_ASIGNADA;" +
    "CONTEO_CABECERA_ASIGNADO_ENVIADO_POR_EL_SISTEMA;" +
    "CONTEO_CABECERA_POR_ARCHIVO;" +
    "CABECERA_IDENTIFICADA_EN_EL_ARCHIVO;" +
    "CABECERAS_IGUALES;" +
    "NOMBRE_DE_DIRECTORIO;" +
    "RUTA_EN_LANDING_RAW_ARCHIVO_SIN_TRANSFORMAR;" +
    "TAMANO_DE_ARCHIVO_(BYTES);" +
    "VALIDACION_SHA;" +
    "FORMATO_DE_ALMACENAMIENTO_DE_ARCHIVO_TRANSFORMADO;" +
    "NOMBRE_TABLA;" +
    "RUTA_EN_LANDINGRAW;" +
    "TOTAL_COLUMNAS_PREPARACION_DE_MARCO_DE_DATOS;" +
    "TOTAL_REGISTROS_PREPARACION_DE_MARCO_DE_DATOS;" +
    "TOTAL_COLUMNAS_OFICIAL;" +
    "TOTAL_REGISTROS_OFICIAL;" +
    "ESTADO_DEL_PROCESO;").split(";")

  private def readFiles: Unit = {
    val stream: FSDataInputStream = fs.open(new Path("/home/proyecto/Reports_Generator/reports/prueba.txt"))
    val source = Source.fromInputStream(stream)
    val sc = sparkSession.sparkContext
    val rdd = sc.parallelize(source.getLines.toSeq)
    var concat = ""
    val delimiterRow = "Ç"
    val splitByChar = ":"
    val limitBlock = "status"

//    source.getLines.foreach(line=>{
//      val l = line.split(splitByChar)
//            concat = s"$concat$delimiterRow${l(1)}"
//            var row = ("", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "")
//            println(s"-->CONCAT: $concat")
//            if (Pattern.compile(s"\\b($limitBlock)\\b").matcher(l(0)).find) {
//              println(s"-->FINAL_CONCAT: $concat")
//              row = splitInfo(concat.split(delimiterRow))
//              concat = ""
//            }
//            row
//    })


    val rowsReport = rdd.repartition(1).map(f = line => {
      println(line)
      val l = line.split(splitByChar)
      concat = s"$concat$delimiterRow${l(1)}"
      var row = ("", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "")
      println(s"-->CONCAT: $concat")
      if (Pattern.compile(s"\\b($limitBlock)\\b").matcher(l(0)).find) {
        println(s"-->FINAL_CONCAT: $concat")
        row = splitInfo(concat.split(delimiterRow))
        concat = ""
      }
      row
    })
    import sparkSession.implicits._
    val df = sc.parallelize(rowsReport.collect.filter { case (s, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) => s.trim.nonEmpty }.toSeq).toDF(columnsReport: _*)
    df.show(1000)
    stream.close()
    source.close()
  }

  private def splitInfo(f: Array[String]) = {
    (f(1).trim, f(2).trim, f(3).trim, f(4).trim, f(5).trim, f(6).trim, f(7).trim,f(8).trim, f(11).trim, f(12).trim,
      f(13).trim, f(14).trim, f(15).trim, f(16).trim, f(17).trim, f(18).trim, f(19).trim, f(20).trim, f(21).trim, f(22).trim)
  }
}
