package com.laomei.ddl.parser

import java.util

/**
  * @author laomei on 2018/11/11 21:14
  */
class Tables(val schemaName: String) {

  private val tables: util.Map[String, Table] = new util.HashMap[String, Table]()

  def addTable(table: Table): Tables = {
    tables.put(table.tableName, table)
    this
  }

  def addTables(tables: List[Table]): Tables = {
    tables.foreach(addTable)
    this
  }

  def getTables: List[Table] = {
    import scala.collection.JavaConverters._
    this.tables.values().asScala.toList
  }

  def getTableByName(name: String): Table = {
    tables.get(name)
  }

  def dropTable(table: String): Table = {
    tables.remove(table)
  }
}
