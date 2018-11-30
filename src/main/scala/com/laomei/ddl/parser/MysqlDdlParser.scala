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
    parseSemicolon()
  }

  private def parseSemicolon(): Unit = {
    if (stream.canConsume(";")) {
      stream.consume(";")
    }
  }

  private def parseUnknownDdl(): Unit = {
    throw new UnsupportedOperationException("Unknown ddl")
  }

  private def parseAlterDdl(): Unit = {
    stream.consume("ALTER")
    stream.consume("TABLE")
    val tableName = stream.consume
    val table = tables.getTableByName(tableName)
    var hasNext = parseAlterSpecification(table)
    while (hasNext) {
      hasNext = parseAlterSpecification(table)
    }
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

  private def parseAlterSpecification(table: Table): Boolean = {
    val token = stream.consume
    token match {
      case "ADD"                => parseAlterAdd(table)
      case "CHANGE"             => parseAlterChange(table)
      case "MODIFY"             => parseAlterModify(table)
      case "ALGORITHM"          => parseTableOptional()
      case "ALTER"              =>
        if (stream.canConsume("COLUMN")) {
          stream.consume("COLUMN")
        }
        stream.consume
        if (stream.canConsume("SET")) {
          stream.consume("SET")
          stream.consume("DEFAULT")
          stream.consume
        } else {
          stream.consume("DROP")
          stream.consume("DEFAULT")
        }
        parseHashNext()
      case "CONVERT"            =>
        stream.consume("TO")
        stream.consume("CHARACTER")
        stream.consume("SET")
        stream.consume
        if (stream.canConsume("COLLATE ")) {
          stream.consume("COLLATE")
          stream.consume
        }
        parseHashNext()
      case "DISABLE"            =>
        stream.consume
        parseHashNext()
      case "ENABLE"             =>
        stream.consume
        parseHashNext()
      case "DISCARD"            =>
        stream.consume
        parseHashNext()
      case "IMPORT"             =>
        stream.consume
        parseHashNext()
      case "DROP"               => parseAlertDrop(table)
      case "FORCE"              => parseHashNext()
      case "LOCK"               => parseTableOptional()
      case "ORDER"              =>
        stream.consume("BY")
        stream.consume
        var end = false
        while (!end) {
          if (stream.canConsume(",")) {
            stream.consume(",")
            stream.consume
          } else {
            end = true
          }
        }
        parseHashNext()
      case "RENAME"             =>
        if (stream.canConsume("TO") || stream.canConsume("AS")) {
          stream.consume
          stream.consume
        } else {
          stream.consume
          stream.consume("TO")
          stream.consume
        }
        parseHashNext()
      case "WITHOUT"            =>
        stream.consume
        parseHashNext()
      case "WITH"               =>
        stream.consume
        parseHashNext()
      case default              => {
        var (hasNext, isTableOption) = parseTableOptions(default, table)
        while (isTableOption) {
          val (hasNext1, isTableOption1) = parseTableOptions(stream.consume, table)
          isTableOption = isTableOption1
          hasNext = hasNext1
        }
        hasNext
      }
    }
  }

  private def parseTableOptions(token: String, table: Table): (Boolean, Boolean) = {
    parseTableOption(token, table)
    var hasNext = false
    if (stream.canConsume(",")) {
      stream.consume(",")
      hasNext = true
    }
    if (isAlterTableOption) {
      return (hasNext, true)
    }
    (hasNext, false)
  }

  private def parseTableOption(token: String, table: Table): Boolean = {
    token match {
      case "AUTO_INCREMENT"     => parseTableOptional()
      case "AVG_ROW_LENGTH"     => parseTableOptional()
      case "CHARACTER"          =>
        stream.consume("SET")
        if (stream.canConsume("=")) {
          stream.consume("=")
        }
        stream.consume
        if (stream.canConsume("COLLATE")) {
          stream.consume("COLLATE")
          if (stream.canConsume("=")) {
            stream.consume("=")
          }
          stream.consume
        }
        parseHashNext()
      case "CHECKSUM"           => parseTableOptional()
      case "COLLATE"            => parseTableOptional()
      case "COMMENT"            => parseTableOptional()
      case "COMPRESSION"        => parseTableOptional()
      case "CONNECTION"         => parseTableOptional()
      case "DATA"               =>
        stream.consume("DIRECTORY")
        parseTableOptional()
      case "INDEX"              =>
        stream.consume("DIRECTORY")
        parseTableOptional()
      case "DELAY_KEY_WRITE"    => parseTableOptional()
      case "ENCRYPTION"         => parseTableOptional()
      case "ENGINE"             => parseTableOptional()
      case "INSERT_METHOD"      => parseTableOptional()
      case "KEY_BLOCK_SIZE"     => parseTableOptional()
      case "MAX_ROWS"           => parseTableOptional()
      case "MIN_ROWS"           => parseTableOptional()
      case "PACK_KEYS"          => parseTableOptional()
      case "PASSWORD"           => parseTableOptional()
      case "ROW_FORMAT"         => parseTableOptional()
      case "STATS_AUTO_RECALC"  => parseTableOptional()
      case "STATS_PERSISTENT"   => parseTableOptional()
      case "STATS_SAMPLE_PAGES" => parseTableOptional()
      case "TABLESPACE"         =>
        stream.consume
        if (stream.canConsume("STORAGE")) {
          stream.consume
          stream.consume
        }
        parseHashNext()
      case "UNION"              =>
        stream.consume
        var end = false
        while (!end) {
          if (stream.canConsume(",")) {
            stream.consume
            stream.consume
          } else {
            end = true
          }
        }
        parseHashNext()
      case "DEFAULT"            =>
        stream.consume match {
          case "CHARACTER" => {
            stream.consume("SET")
            if (stream.canConsume("=")) {
              stream.consume("=")
            }
            stream.consume
            if (stream.canConsume("COLLATE")) {
              stream.consume("COLLATE")
              if (stream.canConsume("=")) {
                stream.consume("=")
              }
              stream.consume
            }
            parseHashNext()
          }
          case "COLLATE"        => parseTableOptional()
        }
    }
  }

  private def parseCreateDdl(): Unit = {
    stream.consume("CREATE")
    if (stream.canConsume("TABLE")) {
      parseCreateTable()
    }
  }

  private def parseAlertDrop(table: Table): Boolean = {
    if (stream.canConsume("PRIMARY")) {
      stream.consume("PRIMARY")
      stream.consume("KEY")
      table.dropPrimaryKey()
    } else if (stream.canConsume("FOREIGN")) {
      stream.consume("FOREIGN")
      stream.consume("KEY")
      stream.consume
    } else if (stream.canConsume("INDEX") || stream.canConsume("KEY")) {
      stream.consume
      stream.consume
    } else {
      if (stream.canConsume("COLUMN")) {
        stream.consume("COLUMN")
      }
      val columnName = stream.consume
      table.dropColumn(columnName)
    }
    parseHashNext()
  }

  private def parseAlterModify(table: Table): Boolean = {
    if (stream.canConsume("COLUMN")) {
      stream.consume("COLUMN")
    }
    val column = stream.consume //column name
    val hasNext = parseColumnDefinition(column, table)
    if (hasNext) {
      return hasNext
    }
    if (stream.canConsume("FIRST")) {
      stream.consume("FIRST")
    } else if (stream.canConsume("AFTER")) {
      stream.consume("AFTER")
      stream.consume
    }
    parseHashNext()
  }

  private def parseAlterChange(table: Table): Boolean = {
    if (stream.canConsume("COLUMN")) {
      stream.consume("COLUMN")
    }
    val oldColumnName = stream.consume
    val newColumnName = stream.consume
    val column = table.columnWithName(oldColumnName)
    table.dropColumn(oldColumnName)
    column.name(newColumnName)
    table.addColumn(column)
    val hasNext = parseColumnDefinition(newColumnName, table)
    if (hasNext) {
      return hasNext
    }
    if (stream.canConsume("FIRST")) {
      stream.consume("FIRST")
    } else if (stream.canConsume("AFTER")) {
      stream.consume("AFTER")
      stream.consume
    }
    parseHashNext()
  }

  private def parseAlterAdd(table: Table): Boolean = {
    if (stream.canConsume("SPATIAL")) {
      stream.consume
      parseAlterAddSpatialOrFullTextOrUnique()
    } else if (stream.canConsume("FULLTEXT")) {
      stream.consume
      parseAlterAddSpatialOrFullTextOrUnique()
    } else if (stream.canConsume("UNIQUE")) {
      stream.consume
      parseAlterAddSpatialOrFullTextOrUnique()
    } else if (stream.canConsume("INDEX") || stream.canConsume("KEY")) {
      parseAlterAddSpatialOrFullTextOrUnique()
    } else if (stream.canConsume("PRIMARY")) {
      parseAlterAddPrimaryKey(table)
    } else if (stream.canConsume("FOREIGN")) {
      parseAlterAddForeignKey()
    } else if (stream.canConsume("CONSTRAINT")) {
      stream.consume("CONSTRAINT")
      parseAlterAddConstraint(table)
    } else {
      //COLUMN
      if (stream.canConsume("COLUMN")) {
        stream.consume("COLUMN")
      }
      var (hasNext, isAlter) = parseColumnAndDefinition(table)
      while (isAlter) {
        val (hasNext1, isAlter1) = parseColumnAndDefinition(table)
        isAlter = isAlter1
        hasNext = hasNext1
      }
      hasNext
    }
  }

  private def parseColumnAndDefinition(table: Table): (Boolean, Boolean) = {
    val columnName = stream.consume
    val column = new Column
    column.name(columnName)
    table.addColumn(column)
    val hasNext: Boolean = parseColumnCreateDefinition(table)
    if (stream.canConsume("FIRST")) {
      stream.consume("FIRST")
    } else if (stream.canConsume("AFTER")) {
      stream.consume("AFTER")
      stream.consume
    }
    if (isAlterSpecification) {
      return (hasNext, true)
    }
    (hasNext, false)
  }

  private def isAlterSpecification: Boolean = {
    if (
      stream.canConsume("ADD")
      || stream.canConsume("ALGORITHM")
      || stream.canConsume("ALTER")
      || stream.canConsume("CHANGE")
      || stream.canConsume("DEFAULT")
      || stream.canConsume("CHARACTER")
      || stream.canConsume("CONVERT")
      || stream.canConsume("DISABLE")
      || stream.canConsume("ENABLE")
      || stream.canConsume("DISCARD")
      || stream.canConsume("IMPORT")
      || stream.canConsume("DROP")
      || stream.canConsume("FORCE")
      || stream.canConsume("MODIFY")
      || stream.canConsume("LOCK")
      || stream.canConsume("ORDER")
      || stream.canConsume("RENAME")
      || stream.canConsume("WITHOUT")
      || stream.canConsume("WITH")
      || isAlterTableOption
    ) {
      return true
    }
    false
  }

  private def isAlterTableOption: Boolean = {
    if (
      stream.canConsume("AUTO_INCREMENT")
      || stream.canConsume("AVG_ROW_LENGTH")
      || stream.canConsume("CHECKSUM")
      || stream.canConsume("COMMENT")
      || stream.canConsume("COMPRESSION")
      || stream.canConsume("CONNECTION")
      || stream.canConsume("DATA")
      || stream.canConsume("INDEX")
      || stream.canConsume("DELAY_KEY_WRITE")
      || stream.canConsume("ENCRYPTION")
      || stream.canConsume("ENGINE")
      || stream.canConsume("INSERT_METHOD")
      || stream.canConsume("KEY_BLOCK_SIZE")
      || stream.canConsume("MAX_ROWS")
      || stream.canConsume("MIN_ROWS")
      || stream.canConsume("PACK_KEYS")
      || stream.canConsume("PASSWORD")
      || stream.canConsume("ROW_FORMAT")
      || stream.canConsume("STATS_AUTO_RECALC")
      || stream.canConsume("STATS_PERSISTENT")
      || stream.canConsume("STATS_SAMPLE_PAGES")
      || stream.canConsume("TABLESPACE")
      || stream.canConsume("UNION")
    ) {
      return true
    }
      false
  }

  private def parseAlterAddConstraint(table: Table): Boolean = {
    stream.consume match {
      case "PRIMARY" => parseAlterAddPrimaryKey(table)
      case "UNIQUE"  => parseAlterAddSpatialOrFullTextOrUnique()
      case "FOREIGN" => parseAlterAddForeignKey()
      case _         => parseAlterAddConstraint(table)
    }
  }

  private def parseAlterAddForeignKey(): Boolean = {
    stream.consume("KEY")
    stream.consume
    stream.consume
    while (stream.canConsume(",")) {
      stream.consume(",")
      stream.consume
    }
    parseReferenceDefinition()
  }

  private def parseAlterAddPrimaryKey(table: Table): Boolean = {
    stream.consume("KEY")
    if (stream.canConsume("USING")) {
      parseIndexType()
    }
    var columnName = parseKeyPart()
    table.columnWithName(columnName).isPk(true)
    while (stream.canConsume(",")) {
      stream.consume
      columnName = parseKeyPart()
      table.columnWithName(columnName).isPk(true)
    }
    var end = false
    while (!end) {
      if (isIndexOption()) {
        parseIndexOption()
      } else {
        end = true
      }
    }
    parseHashNext()
  }

  private def parseAlterAddSpatialOrFullTextOrUnique(): Boolean = {
    if (stream.canConsume("INDEX") || stream.canConsume("KEY")) {
      stream.consume
    }
    stream.consume //index name
    parseKeyPart()
    while (stream.canConsume(",")) {
      stream.consume
      parseKeyPart()
    }
    var end = false
    while (!end) {
      if (isIndexOption()) {
        parseIndexOption()
      } else {
        end = true
      }
    }
    parseHashNext()
  }

  private def parseTableOptional(): Boolean = {
    if (stream.canConsume("=")) {
      stream.consume("=")
    }
    stream.consume
    parseHashNext()
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
    var hasNext = parseColumnCreateDefinition(table)
    while (hasNext) {
      hasNext = parseColumnCreateDefinition(table)
    }
    stream.consume(")")
  }

  private def parseColumnCreateDefinition(table: Table): Boolean = {
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
      case default      => parseColumnDefinition(default, table)
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

  private def parseKeyPart(): String = {
    val name = stream.consume //column name
    if (stream.canConsume("(")) {
      stream.consume //length
      stream.consume(")")
    }
    if (stream.canConsume("ASC") || stream.canConsume("DESC")) {
      stream.consume
    }
    name
  }

  private def isIndexOption(): Boolean = {
    if (
      stream.canConsume("KEY_BLOCK_SIZE")
        || stream.canConsume("WITH")
        || stream.canConsume("COMMENT")
        || stream.canConsume("USING")
    ) {
      return true
    }
    false
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

  private def parseColumnDefinition(columnName: String, table: Table): Boolean = {
    var column = table.columnWithName(columnName)
    if (column == null) {
      column = new Column
      column.name(columnName)
    }
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
