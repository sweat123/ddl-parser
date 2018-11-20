package com.laomei.ddl.parser

import org.junit.jupiter.api.Test

/**
  * @author laomei on 2018/11/20 22:28
  */
class CreateStatementTest extends {

  val ddlParser = new MysqlDdlParser

  val sql = "CREATE TABLE TEST_TABLE (  " +
    "A TINYINT UNSIGNED NULL DEFAULT 0,\n  " +
    "B TINYINT UNSIGNED NULL DEFAULT '10',\n  " +
    "C TINYINT UNSIGNED NULL,\n  " +
    "D TINYINT UNSIGNED NOT NULL,\n  " +
    "E TINYINT UNSIGNED NOT NULL DEFAULT 0,\n  " +
    "F TINYINT UNSIGNED NOT NULL DEFAULT '0'\n" +
    ");"

  @Test
  def testParseCreateTableSql(): Unit = {
    val tables = new Tables("TEST_TABLE")
    ddlParser.parse(sql, tables)
    tables.getTables.foreach(table => {
      table.columns.foreach(column => {
        println(column.toString)
      })
    })
  }
}
