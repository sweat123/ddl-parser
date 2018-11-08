package com.laomei.ddl.parser

import java.util

/**
  * @author laomei on 2018/11/8 22:37
  */
class Table(
           val schemaName: String,
           val tableName: String,
           val columns: Seq[Column]
           ){

  private lazy val columnsByName: util.Map[String, Column] = getColumnsByName

  def columnWithName(name: String): Column = {
    columnsByName.get(name)
  }

  def editor: TableEditor = {
    //todo: return table editor
    null
  }

  private def getColumnsByName: util.Map[String, Column] = {
    import scala.collection.JavaConverters._
    columns.map(c => (c.name.toLowerCase, c)).toMap.asJava
  }
}

object Table {

  def newEditor: TableEditor = {

    //todo return a new table editor
    null
  }
}
