package com.laomei.ddl.parser

/**
  * A definition of a column
  *
  * @author laomei on 2018/11/3 14:37
  */
trait Column {

  /**
    * column name
    */
  def name: String

  /**
    * jdbc type
    */
  def jdbcType: Int

  /**
    * jdbc type name
    */
  def jdbcTypeName: String

  /**
    * scala type
    */
  def scalaType: Int

  /**
    * the length of current column
    */
  def length: Int

  /**
    * current column allow null or not
    */
  def isOptional: Boolean

  /**
    * Determine whether current column has a default value
    */
  def hasDefaultValue: Boolean

  /**
    * Get the default value current column
    */
  def defaultValue: scala.Any
}
