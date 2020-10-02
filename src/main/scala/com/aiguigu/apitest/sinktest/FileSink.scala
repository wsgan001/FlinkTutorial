package com.aiguigu.apitest.sinktest

import com.aiguigu.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object FileSink {
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

    dataStream.print()
//    dataStream.writeAsCsv("out.txt")
    dataStream.addSink(
      StreamingFileSink.forRowFormat(new Path("out"),
      new SimpleStringEncoder[SensorReading]()).build())

    env.execute("FileSinkTest")
  }
}
