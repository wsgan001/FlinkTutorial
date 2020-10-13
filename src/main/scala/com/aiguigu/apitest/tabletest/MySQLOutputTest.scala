package com.aiguigu.apitest.tabletest

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.descriptors._

object MySQLOutputTest {
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

    // 需要保证mysql test数据库下有sensor_count表
    val sinkDDL: String =
      """
        |CREATE TABLE jdbcOutputTable(
        |  id varchar(20) not null,
        |  cnt bigint not null
        |) with (
        |  'connector.type' = 'jdbc',
        |  'connector.url' = 'jdbc:mysql://node01:3306/test',
        |  'connector.table' = 'sensor_count',
        |  'connector.driver' = 'com.mysql.jdbc.Driver',
        |  'connector.username' = 'root',
        |  'connector.password' = '123456'
        |)
        |""".stripMargin
    tableEnv.sqlUpdate(sinkDDL)
    aggTable.insertInto("jdbcOutputTable")

    env.execute()
  }
}