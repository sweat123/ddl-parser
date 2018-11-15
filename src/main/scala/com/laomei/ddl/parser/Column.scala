package com.laomei.ddl.parser

/**
  * A definition of a column
  *
  * @author laomei on 2018/11/3 14:37
  */
class Column(
            val name: String,
            val jdbcType: Int,
            val jdbcTypeName: String,
            val length: Int,
            val isOptional: Boolean,
            val isAutoIncremented: Boolean,
            val hasDefaultValue: Boolean,
            val defaultValue: Any
            ) {

  def edit: ColumnEditor = {
    val editor: ColumnEditor = Column.newEditor
        .name(name)
        .jdbcType(jdbcType)
        .jdbcTypeName(jdbcTypeName)
        .length(length)
        .isOptional(isOptional)
        .isAutoIncremented(isAutoIncremented)
    if (hasDefaultValue) {
      editor.defaultValue(defaultValue)
    }
    editor
  }

  override def equals(obj: scala.Any): Boolean = {
    if (obj == this) {
      return true
    }
    obj match {
      case that: Column =>
        this.name.equalsIgnoreCase(that.name)&&
          this.jdbcType.equals(that.jdbcType)&&
          this.jdbcTypeName.equalsIgnoreCase(that.jdbcTypeName)&&
          this.length.equals(that.length)&&
          this.isOptional.equals(that.isOptional)&&
          this.isAutoIncremented.equals(that.isAutoIncremented)&&
          this.hasDefaultValue.equals(that.hasDefaultValue)&&
          this.defaultValue.equals(that.defaultValue)
      case _ => false
    }
  }

  override def toString: String = {
    val sb = new StringBuilder
    sb.append(" ").append(jdbcTypeName)
    if (length >= 0) {
      sb.append('(').append(length)
      sb.append(')')
    }
    if (!isOptional) sb.append(" NOT NULL")
    if (isAutoIncremented) sb.append(" AUTO_INCREMENTED")
    if (hasDefaultValue && defaultValue == null) {
      sb.append(" DEFAULT VALUE NULL")
    } else if (defaultValue != null) {
      sb.append(" DEFAULT VALUE ").append(defaultValue)
    }
    sb.toString()
  }
}

object Column {

  def newEditor: ColumnEditor = {
    new ColumnEditor
  }
}