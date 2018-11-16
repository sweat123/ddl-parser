package com.laomei.ddl.parser

import java.io.{IOException, StringReader}

/**
  * @author laomei on 2018/11/16 19:08
  */
class MysqlTokenReader {
  private var reader: StringReader = _

  private var currentChar = 0

  def this(str: String) {
    this()
    reader = new StringReader(str)
    commit()
  }

  def next: Char = currentChar.toChar

  @throws[IOException]
  def commit(): Unit = {
    currentChar = reader.read
  }

  def hasNext: Boolean = currentChar != -1
}
