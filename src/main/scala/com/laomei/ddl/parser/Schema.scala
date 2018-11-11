package com.laomei.ddl.parser

import java.util
import scala.collection.mutable.ListBuffer

/**
  * @author laomei on 2018/11/11 21:14
  */
class Schema(val schemaName: String) {

  private val tables: util.Map[String, Table] = new util.HashMap[String, Table]()

  @throws[IllegalStateException]("if the current schema is not same as giving table's")
  def addTable(table: Table): Schema = {
    if (this.schemaName != table.schemaName) {
      throw new IllegalStateException("current schema is not same as giving table schema")
    }
    tables.put(table.tableName.toLowerCase, table)
    this
  }

  def addTables(tables: List[Table]): Schema = {
    tables.foreach(addTable)
    this
  }

  def getTables: List[Table] = {
    import scala.collection.JavaConverters._
    this.tables.values().asScala.toList
  }

  def getTableByName(name: String): Table = {
    tables.get(name.toLowerCase)
  }
}
