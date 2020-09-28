package com.aiguigu.wc

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val inputPath = "/Users/liuyang/projects/learn/FlinkTutorial/src/main/resources/hello.txt"
    val inputDataSet: DataSet[String] = env.readTextFile(inputPath)
    val resultDataSet = inputDataSet
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    resultDataSet.print()
  }
}