### Flink Table API和SQL

----

#### 基本概念

- Table API 一套内嵌在Java和Scala语言中的查询API，允许以直观的方式组合来自一些关系运算符的查询
- Flink SQL 支持基于实现了SQL标准的Apache Calcite

Table API和Flink SQL依赖

```xml
<!-- Flink -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-planner_2.12</artifactId>
    <version>1.10.1</version>
</dependency>
<!-- Blink -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-planner-blink_2.12</artifactId>
    <version>1.10.1</version>
 </dependency>
```

### 基本程序结构

创建表执行环境(TableEnvironment)

```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tableEnv = StreamTableEnvironment.create(env)

// 1.1 基于老版本planner的流处理
val settings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
val oldStreamTableEnv = StreamTableEnvironment.create(env, settings)

// 1.2 基于老版本planner的批处理
val batchEnv = ExecutionEnvironment.getExecutionEnvironment
val oldBatchTableEnv = BatchTableEnvironment.create(batchEnv)

// 1.3 基于blink planner的流处理
val blinkStreamSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
val blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings)

// 1.4 基于blink planner的批处理
val blinkBatchSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
val blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings)
```

创建一张表用于读取数据

```scala
// 把inputTableName注册到当前环境中，接下来的代码中可以使用
tableEnv.connect(SourceConnectorDescriptor).createTemporaryTable("inputTableName")
```

创建一张表用于输出数据

```scala
// 把outputTableName注册到当前环境中，接下来的代码中可以使用
tableEnv.connect(SinkConnectorDescriptor).createTemporaryTable("outputTableName")
```

通过Table API查询算子，得到一个结果表

```scala
val resultTable = tableEnv
    .from("inputTableName")
    .select(...) // SELECT
    .filter(...) // WHERE
```

通过SQL语句，得到一个结果表

```
val sqlResult = tableEnv.sqlQuery("SELECT ... FROM inputTableName ...")
```

将结果写入输出表中

```
resultTable.insertInto("outputTableName")
```

### Flink SQL中的表

- TableEnvironment中可注册Catalog，可以基于Catalog注册表
- 表: catalog.database.tablename(default.default.tablename)
- 常规表(Table)、虚拟表(View)
- 常规表: 描述外部数据，比如文件、数据库、消息队列或者直接从DataStream转换而来
- 视图可以从现有表中创建，通常是Table API或SQL查询的一个结果集

从不同的源创建表

```scala
tableEnv
    .connect(...)    // 定义表的数据源
    .withFormat(...) // 定义数据格式化方法
    .withSchema(...) // 定义表结构
    .createTemporaryTable("tableName") // 创建临时表

// 2. 连接外部系统，读取数据，注册表
// 2.1 读取文件
val filePath = getClass.getResource("/sensor.txt").getPath
tableEnv
    .connect(new FileSystem().path(filePath))
    .withFormat(new Csv()) // 依赖: flink-csv
    .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
    )
    .createTemporaryTable("fileInputTable")

// 先转Table再转成DataStream, 注意需要隐式转换
val fileInputTable: Table = tableEnv.from("fileInputTable")
inputTable.toAppendStream[(String, Long, Double)].print("fileInputTable")

// 2.2 从Kafka读取数据，依赖: flink-connector-kafka-0.11_2.12
tableEnv
    .connect(new Kafka()
        .version("0.11")
        .topic("topicName")
        .property("zookeeper.connect", "zookeeper:2181")
        .property("bootstrap.servers", "kafka:9092")
    )
    .withFormat(new Csv())
    .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
    )
    .createTemporaryTable("kafkaInputTable")
val kafkaInputTable: Table = tableEnv.from("kafkaInputTable")
inputTable.toAppendStream[(String, Long, Double)].print("kafkaInputTable")
```

表的查询 - Table API

- Table API集成于Scala和Java内
- 基于Table类，提供一套API，返回一个新的Table对象，表示对输入表应用转换操作的结果
- 有些关系型转换操作，可以由多个方法调用组成，构成链式调用结构

```scala
// 3 查询转换
// 3.1 使用Table API
// 第一种形式
val sensorTable: Table = tableEnv.from("inputTable")
val resultTable: Table = sensorTable
    .select("id, temperature")
    .filter("id == 'sensor_1'")

// 第二种形式
val sensorTable: Table = tableEnv.from("inputTable")
val resultTable: Table = sensorTable
    .select('id, 'temperature)  // 二元组
    .filter('id === "sensor_1") // 注意是 ===

// 3.2 SQL
val resultSqlTable = tableEnv.sqlQuery(
      """
        |SELECT id, temperature
        |FROM inputTable
        |WHERE id='sensor_1'
        """.stripMargin)
resultSqlTable.toAppendStream[(String, Double)].print("resultSqlTable")

```

### 将DataStream转换为表

```scala
// 1. 直接使用SensorReading的结构
val dataStream: DataStream[SensorReading] = ...
val sensorTable: Table = tableEnv.fromDataStream(dataStream)

// 
val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'timestamp, 'termperature)

// 数据类型与Schema的对应关系
// 1. 基于名称(可以调整顺序或重命名)
val sensorTable: Table = 
    tableEnv.fromDataStream(
      dataStream, 'timestamp as 'ts, 'id as 'myId, 'termperature as 'ts)

// 2. 基于位置 myId对应DataStream中的第一个字段，ts对应DataStream中的第二个字段
val sensorTable: Table = 
    tableEnv.fromDataStream(dataStream, 'myId, 'ts)
```

### 创建临时视图

```scala
// 1. 基于DataStream创建临时视图
tableEnv.createTemporaryView("sensorView", dataStream)
tableEnv.createTemporaryView("sensorView", dataStream, 'id, 'temperature, 'timestamp as 'ts)

// 2. 基于Table创建临时视图
tableEnv.createTemporaryView("sensorView", sensorTable)
```

### 输出表

- 表的输出，是通过将数据写入TableSink来实现的
- TableSink是一个接口，支持不同格式的文件格式、存储数据库和消息队列
- 输出表最直接的方法，通过Table.insertInto()方法将一个Table写入注册过的TableSink中

```scala
// 3.1 简单转换
val resultTable = sensorTable
  .select('id, 'temperature)
  .filter('id === "sensor_1")

// 3.2 聚合转换
// 不支持toAppendStream 而是调用toRetractStream，它返回二元组，前面的boolean表示是否失效了
// 不支持写入到Kafka
val aggTable = sensorTable
.groupBy('id) // 基于id分组
.select('id, 'id.count as 'count)
// import org.apache.flink.api.scala._
aggTable.toRetractStream[(String, Long)].print()

// 4. 输出到文件
val outputPath = "output.txt"
tableEnv.connect(new FileSystem().path(outputPath))
  .withFormat(new Csv())
  .withSchema(new Schema()
    .field("id", DataTypes.STRING)
    .field("temperature", DataTypes.DOUBLE())
  )
  .createTemporaryTable("outputTable")

// 简单Table输出到文件
resultTable.insertInto("outputTable")

tableEnv.connect(new FileSystem().path(outputPath))
  .withFormat(new Csv())
  .withSchema(new Schema()
    .field("id", DataTypes.STRING())
    .field("cnt", DataTypes.BIGINT())
  )
  .createTemporaryTable("outputTable2")
// 不支持这种方式 只能有插入的变化 不能有聚合的
// aggTable.insertInto("outputTable2")

// 5. 输出到Kafka仅支持kafkaInputTable 不支持aggTable
tableEnv.connect(new Kafka()
  .version("0.11")
  .topic("sensor")
  .property("zookeeper.connect", "node01:2181")
  .property("bootstrap.servers", "node01:9092")
)
  .withFormat(new Csv())
  .withSchema(new Schema()
    .field("id", DataTypes.STRING)
    .field("timestamp", DataTypes.BIGINT())
    .field("temperature", DataTypes.DOUBLE())
  )
  .createTemporaryTable("KafkaInputTable")
val kafkaInputTable: Table = tableEnv.from("KafkaInputTable")
tableEnv.connect(new Kafka()
  .version("0.11")
  .topic("kafkaSinkTest") // 一个新的topic
  .property("zookeeper.connect", "node01:2181")
  .property("bootstrap.servers", "node01:9092")
)
  .withFormat(new Csv())
  .withSchema(new Schema()
    .field("id", DataTypes.STRING)
    .field("temperature", DataTypes.DOUBLE())
  )
  .createTemporaryTable("KafkaOutputTable")

resultTable.insertInto("KafkaOutputTable")

// 6. 输出到ElasticSearch
tableEnv.connect(
    new Elasticsearch()
      .version("6")
      .host("node01", 9200, "http")
      .index("sensor")
      .documentType("temp")
)
  .inUpsertMode()
  .withFormat(new Json()) // 依赖: flink-json
  .withSchema(new Schema()
      .field("id", DataTypes.STRING())
      .field("count", DataTypes.BIGINT())
  )
  .createTemporaryTable("esOutputTable")
aggTable.insertInto("esOutputTable")
// 通过命令 curl "node01:9200/sensor/_search?pretty" 查看

// 7. 输出到MySQL，依赖: flink-jdbc_2.12
// 需要保证mysql test数据库下有sensor_count表
val sinkDDL: String =
  """
    |CREATE TABLE jdbcOutputTable(
    |  id varchar(20) not null,
    |  cnt bigint not null
    |) with (
    |  'connector.type'='jdbc',
    |  'connector.url'='jdbc:mysql://node01:3306/test',
    |  'connector.table'='sensor_count',
    |  'connector.driver'='com.mysql.jdbc.Driver',
    |  'connector.username'='root',
    |  'connector.password'='123456',
    |)
    |""".stripMargin
tableEnv.sqlUpdate(sinkDDL)
aggTable.insertInto("jdbcOutputTable")
```

### 更新模式

- 对于流式查询，需要声明如何在表和外部连接器之间进行转换

- 与外部系统交换的消息类型，由更新模式指定

- 更新模式包括

  - 追加模式Append

    表只做插入操作，和外部连接器只交换插入消息

  - 撤回模式Retract

    表和外部连接器交换添加(Add)和撤回(Retract)消息

    插入编码为Add，删除编码为Retract；更新编码为上一条的Retract和下一条的Add消息

  - 更新插入模式Upsert

    更新和插入都编码为Upsert消息，删除编码为Delete消息

### 将Table转换为DataStream

- 表可以转为DataStream或DataSet，这样自定义流处理或批处理程序就可以继续在TableAPI或SQL的结果上继续运行了

- 表转为DataStream或DataSet时需要指定数据类型

  ```scala
  // 1.
  aggTable.toRetractStream[(String, Long)].print()
  resultTable.toAppendStream[(String, Double)]
  // 2.
  aggTable.toRetractStream[Row].print()
  resultTable.toAppendStream[Row]
  ```

- 表做为流式查询的结果，是动态更新的

- 转换模式有两种: 追加模式和撤回模式

### 查看执行计划

```scala
tableEnv.explain(table)
```

### 动态表

- Flink对数据流的TableAPI和SQL支持的核心概念
- 动态表是随时间变化的

   持续查询

- 查询一个动态表会产生持续查询
- 持续查询永远不会终止，会生成另外一个动态表
- 查询会不断更新其动态结果表，以反映其动态输入表上的更改

流式表查询的处理过程

1. 流被转换为动态表
2. 对动态表计算连续查询，生成新的动态表
3. 生成的动态表被转回流

### 动态表转成DataStream

- Dynamic Table -> Retract Stream
- Dynamic Table -> Upsert Stream

### TableAPI和SQL的时间特性

- 基于时间的操作，需要指定时间语义和时间数据来源的信息
- Table可以提供一个逻辑上的时间字段，用于在表处理程序中，指示时间和访问相应的时间戳
- 时间属性，可以是每个表schema的一部分。一旦定义了时间属性，它就可以作为一个字段引用，并且可以在基于时间的操作中使用
- 时间属性的行为类似于常规时间戳，可以访问和进行计算

**定义处理时间(Processing Time)**

不需要提取时间戳，也不需要生成watermark

由DataStream转换成表的时候指定

- 定义Schema时，可以使用.proctime，指定字段名定义处理时间字段
- 该proctime属性只能通过附加逻辑字段来扩展物理Schema，因此只能在Schema的末尾定义它

```scala
val sensorTable = 
    tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp, 'pt.proctime)
```

定义Table Schema时指定

```scala
.withSchema(new Schema()
    .field("id", DataTypes.STRING())
    .field("timestamp", DataTypes.BIGINT())
    .field("temperature", DataTypes.DOUBLE())
    .field("pt", DataTypes.TIMESTAMP(3))
    .proctime()
 )
```

在创建表的DDL中定义

```scala
val sinkDDL: String = 
    """
      |CREATE TABLE dataTable (
      |  id varchar(20) not null,
      |  ts BIGINT,
      |  temperature DOUBLE,
      |  pt AS PROCTIME()
      |) WITH (
      |  'connector.type' = 'filesystem',
      |  'connector.path' = '/sensor.txt',
      |  'format.type' = 'csv',
      |)
    """.stripMargin
tableEnv.sqlUpdate(sinkDDL)
```

