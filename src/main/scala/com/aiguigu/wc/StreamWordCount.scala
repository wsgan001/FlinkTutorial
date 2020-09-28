package com.aiguigu.wc

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.socketTextStream("localhost", 7777)
    val resultDataStream = stream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)
    resultDataStream.print()
    env.execute("StreamWorldCount")
  }
}