package com.laomei.ddl.parser

import org.junit.Assert.assertEquals
import org.junit.Test

/**
  * @author laomei on 2018/11/20 22:28
  */
class DdlParserTest extends {

  val ddlParser = new MysqlDdlParser

  val NUMERIC_SQL =
    "CREATE TABLE NUMERIC_TABLE (\n  " +
      "A TINYINT UNSIGNED NULL DEFAULT 0,\n  " +
      "B SMALLINT UNSIGNED NULL DEFAULT 0,\n  " +
      "C MEDIUMINT UNSIGNED NULL DEFAULT '10',\n  " +
      "D INT UNSIGNED NOT NULL,\n  " +
      "E BIGINT UNSIGNED NOT NULL DEFAULT 0,\n  " +
      "F CHAR(1) DEFAULT NULL,\n  " +
      "G BIT(1) DEFAULT FALSE,\n  " +
      "H BOOLEAN DEFAULT NULL,\n  " +
      "I FLOAT NULL DEFAULT 0,\n  " +
      "J DOUBLE NOT NULL DEFAULT 1.0,\n  " +
      "K REAL NOT NULL DEFAULT 1,\n  " +
      "L NUMERIC(3, 2) NOT NULL DEFAULT 1.23,\n  " +
      "M DECIMAL(4, 3) NOT NULL DEFAULT 2.321\n" +
    ");"

  val DATE_TIME_SQL = "CREATE TABLE DATE_TIME_TABLE (\n  " +
      "A DATE NOT NULL DEFAULT '1976-08-23',\n  " +
      "B TIMESTAMP DEFAULT '1970-01-01 00:00:01',\n  " +
      "C DATETIME DEFAULT '2018-01-03 00:00:10',\n  " +
      "D DATETIME(1) DEFAULT '2018-01-03 00:00:10.7',\n  " +
      "E DATETIME(6) DEFAULT '2018-01-03 00:00:10.123456',\n  " +
      "F YEAR NOT NULL DEFAULT 1,\n " +
      "G TIME DEFAULT '00:00:00',\n  " +
      "H TIME(1) DEFAULT '23:00:00.7',\n  " +
      "I TIME(6) DEFAULT '23:00:00.123456',\n  " +
      "J TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n  " +
      "A1 TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',\n  " +
      "B1 TIMESTAMP NULL DEFAULT '0000-00-00 00:00:00',\n  " +
      "C1 DATE NOT NULL DEFAULT '0000-00-00',\n  " +
      "D1 DATE NULL DEFAULT '0000-00-00',\n  " +
      "E1 DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00',\n  " +
      "F1 DATETIME NULL DEFAULT '0000-00-00 00:00:00'\n" +
    ");"

  val ENUM_AND_SET_TABLE = "CREATE TABLE ENUM_AND_SET_TABLE (\n  " +
      "CUSTOMER_TYPE ENUM ('b2c','b2b') NOT NULL DEFAULT 'b2c',\n  " +
      "A SET('a1', 'a2')\n" +
    ");"

  @Test
  def testParseNumericSql(): Unit = {
    val tables = new Tables("DB")
    ddlParser.parse(NUMERIC_SQL, tables)
    tables.getTables.foreach(table => {
      table.columns.foreach(column => {
        println(column.toString)
      })
    })
  }

  @Test
  def testParseDateSql(): Unit = {
    val tables = new Tables("DB")
    ddlParser.parse(DATE_TIME_SQL, tables)
    tables.getTables.foreach(table => {
      table.columns.foreach(column => {
        println(column.toString)
      })
    })
  }

  @Test
  def testParseEnumAndSetSql(): Unit = {
    val tables = new Tables("DB")
    ddlParser.parse(ENUM_AND_SET_TABLE, tables)
    tables.getTables.foreach(table => {
      table.columns.foreach(column => {
        println(column.toString)
      })
    })
  }

  @Test
  def testDropTable(): Unit = {
    val tables = new Tables("DB")
    ddlParser.parse(NUMERIC_SQL, tables)
    ddlParser.parse(DATE_TIME_SQL, tables)

    val dropTable = "DROP TABLE DATE_TIME_TABLE"
    ddlParser.parse(dropTable, tables)
    val tableSize = tables.getTables.size
    assertEquals(tableSize, 1)
    val tableName = tables.getTables.head.tableName
    assertEquals(tableName, "NUMERIC_TABLE")
  }
}
