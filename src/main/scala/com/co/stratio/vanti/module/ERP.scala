package com.co.stratio.vanti.module

case class ERP
(
  var module: String = "",
  var reportType: String = "",
  var reportPath: String = "",
  var fileGeneratedDate: String = "",
  var file: String = "",
  var tableNameFromFile: String = "",
  var tableNameFromJson: String = "",
  var headersFromJson: String = "",
  var countHeadersFromJson: String = "",
  var countHeadersFromFile: String = "",
  var headersFromFile: String = "",
  var equalsHeaders: String = "",
  var fileDirectory: String = "",
  var filePath: String = "",
  var fileSize: String = "",
  var fileValidSha: String = "",
  var fileColForSchema: String = "",
  var fileTableName: String = "",
  var fileColForPathTable: String = "",
  var fileAntColForCountColumns: String = "",
  var fileAntColForCountRows: String = "",
  var fileColForCountColumns: String = "",
  var fileColForCountRows: String = "",
  var difCountCol: String = "",
  var difCountRow: String = "",
  var status: String = "",
  var generationDate: String = ""
)

