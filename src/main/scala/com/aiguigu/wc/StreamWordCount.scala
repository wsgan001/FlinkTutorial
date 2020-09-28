package com.aiguigu.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 可以突破核心数
    // env.setParallelism(24)

    val paramTool: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = paramTool.get("host")
    val port: Int = paramTool.getInt("port")

    val stream = env.socketTextStream(host, port)
    val resultDataStream = stream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)
    resultDataStream.print().setParallelism(1)
    env.execute("StreamWorldCount")
  }
}