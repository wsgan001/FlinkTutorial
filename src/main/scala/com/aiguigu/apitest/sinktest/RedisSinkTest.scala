package com.aiguigu.apitest.sinktest

import com.aiguigu.apitest.SensorReading
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkTest {
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

    // 定义一个FlinkJedisConfigBase
    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("node01")
      .setPort(6379)
      .build()

    class MyRedisMapper extends RedisMapper[SensorReading] {

      // 写入redis的命令 HSET 表名 key value
      override def getCommandDescription: RedisCommandDescription =
        new RedisCommandDescription(RedisCommand.HSET, "sensor_temp")

      // 将温度值指定为value
      override def getKeyFromData(t: SensorReading): String = t.temperature.toString

      // 将id指定为key
      override def getValueFromData(t: SensorReading): String = t.id
    }

    dataStream.addSink(new RedisSink[SensorReading](conf, new MyRedisMapper()))


    env.execute("RedisSinkTest")
  }
}
