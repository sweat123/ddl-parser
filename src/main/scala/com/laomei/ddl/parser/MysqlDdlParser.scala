package com.laomei.ddl.parser

import java.sql.JDBCType
import java.util.Objects

import scala.util.control.Breaks._

/**
  * @author laomei on 2018/11/3 13:53
  */
class MysqlDdlParser {

  private var stream: MysqlTokenStream = _

  private var tables: Tables = _

  /**
    * parse ddl
    */
  def parse(sql: String, tables: Tables): Unit = {
    this.tables = tables
    stream = new MysqlTokenStream(sql)
    parse()
  }

  private def parse(): Unit = {
    if (stream.canConsume("CREATE")) {
      parseCreateDdl()
    } else if (stream.canConsume("DROP")) {
      parseDropDdl()
    } else if (stream.canConsume("ALTER")) {
      parseAlterDdl()
    } else {
      parseUnknownDdl()
    }
  }

  private def parseUnknownDdl(): Unit = {

  }

  private def parseAlterDdl(): Unit = {
    stream.consume("ALTER")

  }

  private def parseDropDdl(): Unit = {
    stream.consume("DROP")

  }

  private def parseCreateDdl(): Unit = {
    stream.consume("CREATE")
    if (stream.canConsume("TABLE")) {
      parseCreateTable()
    }
  }

  private def parseCreateTable(): Unit = {
    stream.consume("TABLE")
    if (stream.canConsume("TEMPORARY")) {
      stream.consume("TEMPORARY")
    }
    if (stream.canConsume("IF")) {
      stream.consume("IF")
      stream.consume("NOT")
      stream.consume("EXIST")
    }
    var tableName = stream.consume
    if (stream.canConsume(".")) {
      stream.consume(".")
      tableName = stream.consume
    }
    val table = new Table(tableName)
    tables.addTable(table)
    parseCreateContent(table)
  }

  private def parseCreateContent(table: Table): Unit = {
    stream.consume("(")
    parseColumnDefinition(table)
    while (stream.canConsume(",")) {
      stream.consume(",")
      parseColumnDefinition(table)
    }
    stream.consume(")")
  }

  private def parseColumnDefinition(table: Table): Unit = {
    stream.consume match {
      case "PRIMARY"    => parsePrimaryKey(table)
      case "FOREIGN"    => parseForeignKey(table)
      case "INDEX"      => parseIndex()
      case "KEY"        => parseIndex()
      case "UNIQUE"     => parseUniqueIndex(table)
      case "CONSTRAINT" => parseConstraint(table)
      case "SPATIAL"    => parseSpatialOrFullText(table)
      case "FULLTEXT"   => parseSpatialOrFullText(table)
      case "CHECK"      => parseCheck(table)
      case default      => parseColumnCreateDefinition(default, table)
    }
  }

  private def parseSpatialOrFullText(table: Table): Unit = {
    stream.consume match {
      case "INDEX" => parseIndex()
      case "KEY"   => parseIndex()
    }
  }

  /**
    * maybe parse failed
    */
  private def parseCheck(table: Table): Unit = {
    stream.consume("(")
    stream.consume
    stream.consume(")")
  }

  private def parseConstraint(table: Table): Unit = {
    stream.consume match {
      case "PRIMARY" => parsePrimaryKey(table)
      case "UNIQUE"  => parseUniqueIndex(table)
      case "FOREIGN" => parseForeignKey(table)
    }
  }

  private def parseUniqueIndex(table: Table): Unit = {
    if (stream.canConsume("INDEX") || stream.canConsume("KEY")) {
      stream.consume
    }
    parseIndex()
  }

  private def parseIndex(): Unit = {
    stream.consume //index name
    if (stream.canConsume("USING")) {
      stream.consume("USING")
      stream.consume
    }
    stream.consume("(")
    parseKeyPart()
    while (stream.canConsume(",")) {
      stream.consume
      parseKeyPart()
    }
    stream.consume(")")
    parseIndexOption()
  }

  private def parseForeignKey(table: Table): Unit = {
    stream.consume("KEY")
    if (!stream.canConsume("(")) {
      stream.consume  //index name
    }
    stream.consume("(")
    stream.consume
    while (stream.canConsume(",")) {
      stream.consume(",")
      stream.consume
    }
    stream.consume(")")
    stream.consume("REFERENCES")
    stream.consume
    stream.consume("(")
    parseKeyPart()
    while (stream.canConsume(",")) {
      stream.consume
      parseKeyPart()
    }
    stream.consume(")")
  }

  private def parsePrimaryKey(table: Table): Unit = {
    stream.consume("KEY")
    parseIndexType()
    var columnName = ""
    if (stream.canConsume("(")) {
      stream.consume("(")
      columnName = stream.consume
      stream.consume(")")
    } else {
      columnName = stream.consume
    }
    val column = table.columnWithName(columnName)
    column.isPk(true)
    parseIndexOption()
  }

  private def parseKeyPart(): Unit = {
    stream.consume //column name
    if (stream.canConsume("(")) {
      stream.consume //length
      stream.consume(")")
    }
    if (stream.canConsume("ASC") || stream.canConsume("DESC")) {
      stream.consume
    }
  }

  private def parseIndexOption(): Unit = {
    stream.consume match {
      case "KEY_BLOCK_SIZE" =>
        if (stream.canConsume("=")) {
          stream.consume("=")
          stream.consume
        }
      case "WITH"           =>
        stream.consume("PARSER")
        stream.consume
      case "COMMENT"        => stream.consume
      case "USING"          => stream.consume
    }
  }

  private def parseIndexType(): Unit = {
    if (stream.canConsume("USING")) {
      stream.consume("USING")
      stream.consume
    }
  }

  private def parseColumnCreateDefinition(columnName: String, table: Table): Unit = {
    val column = new Column
    val jdbcType = stream.consume
    var length: Int = 0
    if (stream.canConsume("(")) {
      stream.consume("(")
      length = stream.consume.toInt
      stream.consume(")")
    }
    column.name(columnName)
    column.jdbcType(JDBCType.valueOf(jdbcType))
    column.length(length)
    table.addColumn(column)
    parseColumnDefinitionDetail(column)
  }

  private def parseColumnDefinitionDetail(column: Column): Unit = {
    val token = stream.consume
    token match {
      case "NOT" =>
        stream.consume("NULL")
        column.isOptional(true)
        parseColumnDefinitionDetail(column)
      case "NULL" =>
        column.isOptional(false)
        parseColumnDefinitionDetail(column)
      case "DEFAULT" =>
        parseDefaultValue(column)
      case "AUTO_INCREMENT" =>
        column.isAutoIncremented(true)
        parseColumnDefinitionDetail(column)
      case "UNIQUE" =>
        if (stream.canConsume("KEY")) {
          stream.consume("KEY")
        }
        parseColumnDefinitionDetail(column)
      case "PRIMARY" =>
        stream.consume("KEY")
        column.isPk(true)
        parseColumnDefinitionDetail(column)
      case "KEY" =>
        column.isPk(true)
        parseColumnDefinitionDetail(column)
      case "COMMENT" =>
      case "COLUMN_FORMAT" =>
        stream.consume
        parseColumnDefinitionDetail(column)
      case "STORAGE" =>
        stream.consume
      case "REFERENCES" => parseReferenceDefinition()
      case "," => {
        //ignore
      }
    }
  }

  private def parseReferenceDefinition(): Unit = {
    stream.consume
    stream.consume("(")
    parseKeyPart()
    while (stream.canConsume(",")) {
      stream.consume(",")
      parseKeyPart()
    }
    stream.consume(")")
    stream.consume match {
      case "MATCH" => stream.consume
      case "ON" => {
        stream.consume
        parseReferenceOption()
      }
    }
  }

  private def parseReferenceOption(): Unit = {
    if (stream.canConsume("SET") || stream.canConsume("NO")) {
      stream.consume
    }
    stream.consume
  }

  private def parseDefaultValue(column: Column): Unit = {
    if (stream.canConsume("'")) {
      parseDefaultValue(column, "'")
    } else if (stream.canConsume("\"")) {
      parseDefaultValue(column, "\"")
    } else {
      column.defaultValue(stream.consume)
    }
  }

  private def parseDefaultValue(column: Column, str: String): Unit = {
    stream.consume(str)
    if (stream.canConsume(str)) {
      stream.consume(str)
      column.defaultValue("")
      return
    }
    val sb = new StringBuilder
    sb.append(stream.consume)
    breakable {
      while (true) {
        val token = stream.consume
        if (Objects.equals(token, str)) {
          break
        }
        sb.append(" ").append(token)
      }
    }
    column.defaultValue(sb.toString())
  }
}
