package com.laomei.ddl.parser

/**
  * @author laomei on 2018/11/6 22:41
  */
trait ColumnEditor {

  def name: String

  def jdbcType: Int

  def jdbcTypeName: String

  def scalaType: Int

  def length: Int

  def isOptional: Boolean

  def isAutoIncremented: Boolean

  def hasDefaultValue: Boolean

  def defaultValue: Any

  def name(name: String): ColumnEditor

  def jdbcType(jdbcType: Int): ColumnEditor

  def jdbcTypeTypeName(jdbcType: String): ColumnEditor

  def scalaType(scalaType: Int): ColumnEditor

  def length(length: Int): ColumnEditor

  def isOptional(isOptional: Boolean): ColumnEditor

  def isAutoIncremented(isAutoIncremented: Boolean): ColumnEditor

  def hasDefaultValue(hasDefaultValue: Boolean): ColumnEditor

  def defaultValue(defaultValue: Any): ColumnEditor

  def create: Column
}
