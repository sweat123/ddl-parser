package com.laomei.ddl.parser

/**
  * @author laomei on 2018/11/6 22:41
  */
class ColumnEditor {

  var name: String = _

  var jdbcType: Int = _

  var jdbcTypeName: String = _

  var scalaType: Int = _

  var length: Int = _

  var scale: Int = _

  var isOptional: Boolean = true

  var isAutoIncremented: Boolean = false

  var hasDefaultValue: Boolean = false

  var defaultValue: Any = null

  def name(name: String): ColumnEditor = {
    this.name = name
    this
  }

  def jdbcType(jdbcType: Int): ColumnEditor = {
    this.jdbcType = jdbcType
    this
  }

  def jdbcTypeName(jdbcTypeName: String): ColumnEditor = {
    this.jdbcTypeName = jdbcTypeName
    this
  }

  def scalaType(scalaType: Int): ColumnEditor = {
    this.scalaType = scalaType
    this
  }

  def length(length: Int): ColumnEditor = {
    this.length = length
    this
  }

  def scale(scale: Int): ColumnEditor = {
    this.scale = scale
    this
  }

  def isOptional(isOptional: Boolean): ColumnEditor = {
    this.isOptional = isOptional
    if (isOptional && !hasDefaultValue) {
      defaultValue = null
    }
    this
  }

  def isAutoIncremented(isAutoIncremented: Boolean): ColumnEditor = {
    this.isAutoIncremented = isAutoIncremented
    this
  }

  def defaultValue(defaultValue: Any): ColumnEditor = {
    this.defaultValue = defaultValue
    this.hasDefaultValue = true
    this
  }

  def create: Column = {
    new Column(
      name,
      jdbcType,
      jdbcTypeName,
      scalaType,
      length,
      scale,
      isOptional,
      isAutoIncremented,
      hasDefaultValue,
      defaultValue
    )
  }
}
