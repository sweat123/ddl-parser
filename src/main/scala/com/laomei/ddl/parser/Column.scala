package com.laomei.ddl.parser

/**
  * @author laomei on 2018/11/6 22:41
  */
class Column {

  var name: String = _

  var dateType: String = _

  var length: ColumnLength = ColumnLength.defaultColumnLength

  var isPk: Boolean = false

  var isOptional: Boolean = true

  var isAutoIncremented: Boolean = false

  var hasDefaultValue: Boolean = false

  var defaultValue: Any = null

  def name(name: String): Column = {
    this.name = name
    this
  }

  def dateType(dateType: String): Column = {
    this.dateType = dateType
    this
  }

  def length(length: ColumnLength): Column = {
    this.length = length
    this
  }

  def isPk(isPk: Boolean): Column = {
    this.isPk = isPk
    this
  }

  def isOptional(isOptional: Boolean): Column = {
    this.isOptional = isOptional
    if (isOptional && !hasDefaultValue) {
      defaultValue = null
    }
    this
  }

  def isAutoIncremented(isAutoIncremented: Boolean): Column = {
    this.isAutoIncremented = isAutoIncremented
    this
  }

  def defaultValue(defaultValue: Any): Column = {
    this.defaultValue = defaultValue
    this.hasDefaultValue = true
    this
  }

  override def equals(obj: scala.Any): Boolean = {
    if (obj == this) {
      return true
    }
    obj match {
      case that: Column =>
        this.name.equalsIgnoreCase(that.name)&&
          this.dateType.equals(that.dateType)&&
          this.length.equals(that.length)&&
          this.isPk.equals(that.isPk)&&
          this.isOptional.equals(that.isOptional)&&
          this.isAutoIncremented.equals(that.isAutoIncremented)&&
          this.hasDefaultValue.equals(that.hasDefaultValue)&&
          this.defaultValue.equals(that.defaultValue)
      case _ => false
    }
  }

  override def toString: String = {
    val sb = new StringBuilder
    sb.append(name).append(" ").append(dateType)
    if (length.hasDigits || length.length > 0) {
      sb.append(length)
    }
    if (!isOptional) sb.append(" NOT NULL")
    else sb.append(" NULL")
    if (isAutoIncremented) sb.append(" AUTO_INCREMENTED")
    if (hasDefaultValue && defaultValue == null) {
      sb.append(" DEFAULT VALUE NULL")
    } else if (defaultValue != null) {
      sb.append(" DEFAULT VALUE ").append(defaultValue)
    }
    if (isPk) {
      sb.append(" PRIMARY KEY")
    }
    sb.toString()
  }
}

class ColumnLength {
  var hasDigits: Boolean = false
  var length: Int = 0
  var digits: Int = 0

  override def toString: String = {
    val sb = new StringBuilder
    sb.append("(").append(length)
    if (hasDigits) {
      sb.append(",").append(digits)
    }
    sb.append(")")
    sb.toString()
  }
}

object ColumnLength {
  val defaultColumnLength = new ColumnLength
}
