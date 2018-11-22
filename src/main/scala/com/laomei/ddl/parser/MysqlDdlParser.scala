package com.laomei.ddl.parser

import java.util.Objects

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
    if (stream.canConsume("TEMPORARY")) {
      stream.consume("TEMPORARY")
    }
    stream.consume("TABLE")
    if (stream.canConsume("IF")) {
      stream.consume("IF")
      stream.consume("EXISTS")
    }
    var tableName = stream.consume
    tables.dropTable(tableName)
    var canDrop = true
    while (canDrop) {
      if (stream.canConsume(",")) {
        stream.consume(",")
        tableName = stream.consume
        tables.dropTable(tableName)
      } else {
        canDrop = false
      }
    }
    if (stream.canConsume("RESTRICT") | stream.canConsume("CASCADE")) {
      stream.consume
    }
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
    var hasNext = parseColumnDefinition(table)
    while (hasNext) {
      hasNext = parseColumnDefinition(table)
    }
    stream.consume(")")
  }

  private def parseColumnDefinition(table: Table): Boolean = {
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

  private def parseSpatialOrFullText(table: Table): Boolean = {
    stream.consume match {
      case "INDEX" => parseIndex()
      case "KEY"   => parseIndex()
    }
  }

  /**
    * maybe parse failed
    */
  private def parseCheck(table: Table): Boolean = {
    stream.consume("(")
    stream.consume
    stream.consume(")")
    parseHashNext()
  }

  private def parseConstraint(table: Table): Boolean = {
    stream.consume match {
      case "PRIMARY" => parsePrimaryKey(table)
      case "UNIQUE"  => parseUniqueIndex(table)
      case "FOREIGN" => parseForeignKey(table)
    }
  }

  private def parseUniqueIndex(table: Table): Boolean = {
    if (stream.canConsume("INDEX") || stream.canConsume("KEY")) {
      stream.consume
    }
    parseIndex()
  }

  private def parseIndex(): Boolean = {
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
    parseHashNext()
  }

  private def parseForeignKey(table: Table): Boolean = {
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
    parseHashNext()
  }

  private def parsePrimaryKey(table: Table): Boolean = {
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
    parseHashNext()
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

  private def parseColumnCreateDefinition(columnName: String, table: Table): Boolean = {
    val column = new Column
    column.name(columnName)
    parseColumnType(column)
    table.addColumn(column)
    if (stream.canConsume(")")) {
      return false
    }
    parseColumnDefinitionDetail(column)
  }

  private def parseColumnDefinitionDetail(column: Column): Boolean = {
    val token = stream.consume
    token match {
      case "NOT" =>
        stream.consume("NULL")
        column.isOptional(false)
        parseColumnDefinitionDetail(column)
      case "NULL" =>
        column.isOptional(true)
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
        parseComment(column)
        parseColumnDefinitionDetail(column)
      case "COLUMN_FORMAT" =>
        stream.consume
        parseColumnDefinitionDetail(column)
      case "STORAGE" =>
        stream.consume
        parseHashNext()
      case "REFERENCES" => parseReferenceDefinition()
      case "," => true
      case _ => parseHashNext()
    }
  }

  private def parseReferenceDefinition(): Boolean = {
    stream.consume
    stream.consume("(")
    parseKeyPart()
    while (stream.canConsume(",")) {
      stream.consume(",")
      parseKeyPart()
    }
    stream.consume(")")
    stream.consume match {
      case "MATCH" =>
        stream.consume
        parseHashNext()
      case "ON" => {
        stream.consume
        parseReferenceOption()
      }
    }
  }

  private def parseReferenceOption(): Boolean = {
    if (stream.canConsume("SET") || stream.canConsume("NO")) {
      stream.consume
    }
    stream.consume
    parseHashNext()
  }

  private def parseColumnType(column: Column): Unit = {
    val dateType = stream.consume
    column.dateType(dateType)
    dateType match {
      case "DECIMAL" => parseDecimalType(column)
      case "DEC"     => parseDecimalType(column)
      case "NUMERIC" => parseDecimalType(column)
      case "DOUBLE"  => parseDoubleType(column)
      case "FLOAT"   => parseDoubleType(column)
      case "ENUM"    => parseEnumType(column)
      case "SET"     => parseSetType(column)
      case _         => parseType(column)
    }
  }

  private def parseDecimalType(column: Column): Unit = {
    val columnLength = new ColumnLength
    if (stream.canConsume("(")) {
      stream.consume("(")
      columnLength.length = stream.consume.toInt
      if (stream.canConsume(",")) {
        stream.consume(",")
        columnLength.digits = stream.consume.toInt
        columnLength.hasDigits = true
      }
      stream.consume(")")
    }
    column.length = columnLength
    if (stream.canConsume("UNSIGNED")) {
      stream.consume("UNSIGNED")
    }
  }

  private def parseDoubleType(column: Column): Unit = {
    val columnLength = new ColumnLength
    if (stream.canConsume("(")) {
      stream.consume("(")
      columnLength.length = stream.consume.toInt
      columnLength.hasDigits = true
      stream.consume(",")
      columnLength.digits = stream.consume.toInt
      stream.consume(")")
    }
    column.length = columnLength
    if (stream.canConsume("UNSIGNED")) {
      stream.consume("UNSIGNED")
    }
  }

  private def parseEnumType(column: Column): Unit = {
    parseMultipleValues(column)
  }

  private def parseSetType(column: Column): Unit = {
    parseMultipleValues(column)
  }

  private def parseType(column: Column): Unit = {
    val columnLength = new ColumnLength
    columnLength.length = 0
    if (stream.canConsume("(")) {
      stream.consume("(")
      val length = stream.consume
      stream.consume(")")
      columnLength.length = length.toInt
    }
    column.length(columnLength)
    if (stream.canConsume("UNSIGNED")) {
      stream.consume("UNSIGNED")
    }
  }

  private def parseDefaultValue(column: Column): Boolean = {
    if (stream.canConsume("'")) {
      parseDefaultValue(column, "'")
    } else if (stream.canConsume("\"")) {
      parseDefaultValue(column, "\"")
    } else {
      var value = stream.consume
      if (stream.canConsume(".")) {
        value += stream.consume
        value += stream.consume
      }
      column.defaultValue(value)
    }
    parseHashNext()
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
    var finished = false
    while (!finished) {
      val token = stream.consume
      if (Objects.equals(token, str)) {
        finished = true
      } else {
        sb.append(" ").append(token)
      }
    }
    column.defaultValue(sb.toString())
  }

  private def parseComment(column: Column): Boolean = {
    stream.consume
    parseHashNext()
  }

  private def parseMultipleValues(column: Column): Unit = {
    stream.consume("(")
    stream.consume
    var end = false
    while (!end) {
      if (!stream.canConsume(",")) {
        end = true
      } else {
        stream.consume(",")
        stream.consume
      }
    }
    stream.consume(")")
  }

  private def parseHashNext(): Boolean = {
    if (stream.canConsume(",")) {
      stream.consume(",")
      return true
    }
    false
  }
}
