package com.aiguigu.apitest.tabletest

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.descriptors._

object EsOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)

    val inputPath = getClass.getResource("/sensor.txt").getPath
    tableEnv.connect(new FileSystem().path(inputPath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING)
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")

    val sensorTable = tableEnv.from("inputTable")

    val aggTable = sensorTable
      .groupBy('id) // 基于id分组
      .select('id, 'id.count as 'count)

    tableEnv.connect(
      new Elasticsearch()
        .version("6")
        .host("node01", 9200, "http")
        .index("sensor")
        .documentType("temperature")
    )
      .inUpsertMode() // 更新模式为UPSERT 外部系统必须支持
      .withFormat(new Json())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("count", DataTypes.BIGINT())
      )
      .createTemporaryTable("esOutputTable")
    aggTable.insertInto("esOutputTable")
    // curl "localhost:9200/sensor/_search?pretty"

    env.execute()
  }
}