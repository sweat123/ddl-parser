package com.laomei.ddl.parser

import org.slf4j.{Logger, LoggerFactory}

/**
  * @author laomei on 2018/11/3 13:53
  */
class DdlParser {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
    * parse ddl
    */
  def parse(sql: String, tables: Tables): Unit = {

  }

  def parseDataDefinitionStatement(sql: String, tables: Tables): Unit = {

  }
}
