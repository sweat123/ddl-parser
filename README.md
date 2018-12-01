# ddl-parser

Mysql DDL Parser;

1. `git clone repo`
2. `mvn clean install`

在项目里添加以下依赖

```xml
<dependency>
    <groupId>com.laomei.mysql</groupId>
    <artifactId>ddl-parser</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

使用方式

```scala
  val NUMERIC_SQL =
    "CREATE TABLE NUMERIC_TABLE (\n  " +
      "A TINYINT UNSIGNED NULL DEFAULT 0,\n  " +
      "B SMALLINT UNSIGNED NULL DEFAULT 0,\n  " +
      "C MEDIUMINT UNSIGNED NULL DEFAULT '10',\n  " +
      "D INT UNSIGNED NOT NULL,\n  " +
      "E BIGINT UNSIGNED NOT NULL DEFAULT 0,\n  " +
      "F CHAR(1) DEFAULT NULL,\n  " +
      "G BIT(1) DEFAULT FALSE,\n  " +
      "H BOOLEAN DEFAULT NULL,\n  " +
      "I FLOAT NULL DEFAULT 0,\n  " +
      "J DOUBLE NOT NULL DEFAULT 1.0,\n  " +
      "K REAL NOT NULL DEFAULT 1,\n  " +
      "L NUMERIC(3, 2) NOT NULL DEFAULT 1.23,\n  " +
      "M DECIMAL(4, 3) NOT NULL DEFAULT 2.321\n" +
    ");"

  val ddlParser = new MysqlDdlParser

  def testParseNumericSql(): Unit = {
    //db is schema name
    val tables = new Tables("DB")
    ddlParser.parse(NUMERIC_SQL, tables)
    tables.getTables.foreach(table => {
      table.columns.foreach(column => {
        println(column.toString)
      })
    })
  }
```

`result` 

```
A TINYINT NULL DEFAULT VALUE 0
B SMALLINT NULL DEFAULT VALUE 0
C MEDIUMINT NULL DEFAULT VALUE 10
D INT NOT NULL
E BIGINT NOT NULL DEFAULT VALUE 0
F CHAR(1) NULL DEFAULT VALUE NULL
G BIT(1) NULL DEFAULT VALUE FALSE
H BOOLEAN NULL DEFAULT VALUE NULL
I FLOAT NULL DEFAULT VALUE 0
J DOUBLE NOT NULL DEFAULT VALUE 1.0
K REAL NOT NULL DEFAULT VALUE 1
L NUMERIC(3,2) NOT NULL DEFAULT VALUE 1.23
M DECIMAL(4,3) NOT NULL DEFAULT VALUE 2.321
```
