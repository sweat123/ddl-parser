package com.laomei.ddl.parser

import java.util

/**
  * @author laomei on 2018/11/8 22:55
  */
class TableEditor {

  var schemaName: String = _

  var tableName: String = _

  private val sortedColumns: util.Map[String, Column] = new util.LinkedHashMap[String, Column]

  def columns: List[Column] = {
    import scala.collection.JavaConverters._
    sortedColumns.values().asScala.toList
  }

  def columnWithName(name: String): Column = {
    sortedColumns.get(name)
  }

  def hasColumnWithName(name: String): Boolean = {
    columnWithName(name) != null
  }

  def columnNames: List[String] = {
    columns.map(_.name)
  }

  def addColumn(column: Column): TableEditor = {
    sortedColumns.put(column.name.toLowerCase, column)
    this
  }

  def addColumns(columns: List[Column]): TableEditor = {
    columns.foreach(addColumn)
    this
  }

  def create: Table = {
    import scala.collection.JavaConverters._
    val columns = sortedColumns.values().asScala.toList
    new Table(schemaName, tableName, columns)
  }
}
