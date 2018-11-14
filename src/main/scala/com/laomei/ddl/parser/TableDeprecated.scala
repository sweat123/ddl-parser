//package com.laomei.ddl.parser
//
//import java.util
//
///**
//  * @author laomei on 2018/11/8 22:37
//  */
//class Table(
//           val tableName: String,
//           val columns: Seq[Column]
//           ){
//
//  private lazy val columnsByName: util.Map[String, Column] = getColumnsByName
//
//  def columnWithName(name: String): Column = {
//    columnsByName.get(name)
//  }
//
//  def editor: TableEditor = {
//    val editor = new TableEditor
//    editor.tableName = tableName
//    editor.addColumns(columns.toList)
//    editor
//  }
//
//  private def getColumnsByName: util.Map[String, Column] = {
//    import scala.collection.JavaConverters._
//    columns.map(c => (c.name.toLowerCase, c)).toMap.asJava
//  }
//}
//
//object Table {
//
//  def newEditor: TableEditor = {
//    new TableEditor
//  }
//}
