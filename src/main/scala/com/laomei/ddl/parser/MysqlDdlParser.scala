package com.laomei.ddl.parser

import org.slf4j.{Logger, LoggerFactory}

/**
  * @author laomei on 2018/11/3 13:53
  */
class MysqlDdlParser {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  var stream: MysqlTokenStream = _

  var currentToken: Token = _

  var tables: Tables = _

  /**
    * parse ddl
    */
  def parse(sql: String, tables: Tables): Unit = {
    this.tables = tables
    stream = new MysqlTokenStream(sql)
    if (!stream.hasNext) {
      return
    }
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
    var tableName = stream.consume()
    if (stream.canConsume(".")) {
      stream.consume(".")
      tableName = stream.consume()
    }
    val table = new Table(tableName)
    tables.addTable(table)
    parseCreateContent(table)
  }

  private def parseCreateContent(table: Table): Unit = {
    stream.consume("(")
    parseColumnCreateDefinition(table)
    while (stream.canConsume(",")) {
      stream.consume(",")
      parseColumnCreateDefinition(table)
    }
    stream.consume(")")
  }

  private def parseColumnCreateDefinition(table: Table): Unit = {
    val columnName = stream.consume()
    val jdbcType = stream.consume()
    var length: Int = 0
    if (stream.canConsume("(")) {
      stream.consume("(")
      length = stream.consume().toInt
      stream.consume(")")
    }
    var optional: Boolean = true
    if (stream.canConsume("NULL")) {
      stream.consume("NULL")
    } else if (stream.canConsume("NOT")) {
      stream.consume("NOT")
      stream.consume("NULL")
      optional = false
    }
    var isAutoIncrement: Boolean = false
    if (stream.canConsume("AUTO_INCREMENT")) {
      stream.consume("AUTO_INCREMENT")
      isAutoIncrement = true
    }

  }
}
