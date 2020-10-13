package com.aiguigu.apitest.tabletest

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

object KafkaPipelineTest {
  def main(args: Array[String]): Unit = {
    // 1. 环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)

    // 2. 从Kafka读取数据
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

    // 3. 查询转换
    // 3.1 简单转换
    val sensorTable = tableEnv.from("KafkaInputTable")
    val resultTable = sensorTable
      .select('id, 'temperature)
      .filter('id === "sensor_1")

    // 3.2 聚合转换
    val aggTable = sensorTable
      .groupBy('id)
      .select('id, 'id.count as 'count)

    // 4. 输出到Kafka
    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("kafkaSinkTest")
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

    env.execute()
  }
}