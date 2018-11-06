package com.laomei.ddl.parser

/**
  * A definition of a column
  *
  * @author laomei on 2018/11/3 14:37
  */
class Column {

  var name: String = _

  /**
    * jdbc type
    */
  var jdbcType: Int = null

  /**
    * jdbc type name
    */
  var jdbcTypeName: String= _

  /**
    * scala type
    */
  var scalaType: Int = null

  /**
    * the length of current column
    */
  var length: Int = -1

  /**
    * current column allow null or not
    */
  var isOptional: Boolean = null

  /**
    * auto increment or not
    */
  var isAutoIncremented: Boolean = null

  /**
    * Determine whether current column has a default value
    */
  var hasDefaultValue: Boolean = null

  /**
    * Get the default value current column
    */
  var defaultValue: Any = null
}
