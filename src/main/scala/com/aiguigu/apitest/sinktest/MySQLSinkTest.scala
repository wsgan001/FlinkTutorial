package com.aiguigu.apitest.sinktest

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.aiguigu.apitest.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object MySQLSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputPath = getClass.getResource("/sensor.txt").getPath
    val inputStream = env.readTextFile(inputPath)
    val dataStream = inputStream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )
    dataStream.addSink(new MyJdbcSinkFunc)

    env.execute("MySQLSinkTest")
  }
}

class MyJdbcSinkFunc() extends RichSinkFunction[SensorReading] {

  // 定义连接 预编译语句
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _


  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    // 先执行更新 查到就更新
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(1, value.temperature)
      insertStmt.execute()
    }
  }

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306", "root", "123456")
    insertStmt = conn.prepareStatement("INSERT INTO SENSOR_TEMP(id,temp) values (?, ?)")
    updateStmt = conn.prepareStatement("UPDATE SENSOR_TEMP SET temp = ? WHERE id = ?")
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}
