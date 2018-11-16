package com.laomei.ddl.parser

import java.io.IOException
import java.util.Objects
import scala.util.control.Breaks._

/**
  * @author laomei on 2018/11/16 19:10
  */
class MysqlTokenStream {
  private var reader: MysqlTokenReader = _

  private var currentToken: String = _

  def this(content: String) {
    this()
    this.reader = new MysqlTokenReader(content)
    currentToken = nextToken
  }

  def canConsume(token: String): Boolean = Objects.equals(currentToken, token)

  @throws[IOException]
  def consume(token: String): Unit = {
    if (!canConsume(token)) throw new IllegalStateException("can not consume token " + token)
    consume
  }

  @throws[IOException]
  def consume: String = {
    val token = currentToken
    nextToken
    token
  }

  @throws[IOException]
  private
  def nextToken = {
    var token: String = null
    var foundToken = false
    breakable {
      while (!foundToken && reader.hasNext) {
        val nextChar = reader.next
        nextChar match {
          case ' '  =>
          case '\t' =>
          case '\r' =>
          case '\n' =>
            //ignore these character
            reader.commit()
          case '#'  =>
            // comment, we need read next character util '\n' or '\r'
            readComment()
          case '('  =>
          case ')'  =>
          case ','  =>
            token = String.valueOf(nextChar)
            foundToken = true
            reader.commit()
          case '`'  =>
          case '\'' =>
          case '"'  =>
            token = readValueWithQuote(nextChar)
            foundToken = true
          case '-'  =>
            reader.commit()
            if (reader.next == '-') {
              readComment()
            } else {
              foundToken = true
              token = currentToken
            }
          case _    =>
            //word
            val sb = new StringBuilder
            sb.append(currentToken)
            reader.commit()
            breakable {
              while (reader.hasNext) {
                val next = reader.next
                if (Character.isWhitespace(next) || isKeyCharacter(next)) {
                  break
                }
                reader.commit()
              }
            }
            token = sb.toString
            foundToken = true
        }
      }
    }
    token
  }

  private
  def isKeyCharacter(c: Char) = {
    c == ',' ||
      c == ';' ||
      c == '(' ||
      c == ')' ||
      c == '{' ||
      c == '}' ||
      c == '[' ||
      c == ']' ||
      c == '<' ||
      c == '>' ||
      c == '=' ||
      c == '\'' ||
      c == '`' ||
      c == '"' ||
      c == '.' ||
      c == '/' ||
      c == '*' ||
      c == '!' ||
      c == ':' ||
      c == '-' ||
      c == '+' ||
      c == '%' ||
      c == '?'
  }

  @throws[IOException]
  private
  def readValueWithQuote(quote: Char) = {
    reader.commit()
    val sb = new StringBuilder
    breakable {
      while (reader.hasNext) {
        val c = reader.next
        if (c == quote) {
          reader.commit()
          break
        }
        sb.append(c)
        reader.commit()
      }
    }
    sb.toString
  }

  @throws[IOException]
  private
  def readComment(): Unit = {
    reader.commit()
    breakable {
      while (reader.hasNext) {
        val c = reader.next
        reader.commit()
        if (c == '\n' || c == '\r') {
          break
        }
      }
    }
  }
}
