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
    .withFormat(new Csv()) // 需要加入依赖flink-csv
    .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
    )
    .createTemporaryTable("fileInputTable")

// 先转Table再转成DataStream, 注意需要隐式转换
val fileInputTable: Table = tableEnv.from("fileInputTable")
inputTable.toAppendStream[(String, Long, Double)].print("fileInputTable")

// 2.2 从Kafka读取数据，需要引入依赖flink-connector-kafka-0.11_2.12
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

输出表

```scala

```
