package com.aiguigu.apitest.sinktest

import java.util

import com.aiguigu.apitest.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object ElasticSearchSinkTest {
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

    // 定义HttpHosts
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("node01", 9200))

    // 自定义写入es的sink function
    val myEsSinkFunction = new ElasticsearchSinkFunction[SensorReading] {
      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        // 包装一个Map做为data source
        val dataSource = new util.HashMap[String, String]()
        dataSource.put("id", t.id)
        dataSource.put("temperature", t.temperature.toString)
        dataSource.put("ts", t.timestamp.toString)

        // 创建index request
        val indexRequest = Requests.indexRequest()
          .index("sensor")
          .`type`("readingdata")
          .source(dataSource)

        // 用indexer发送请求
        requestIndexer.add(indexRequest)
      }
    }

    dataStream.addSink(
      new ElasticsearchSink.Builder[SensorReading](httpHosts, myEsSinkFunction)
        .build())

    env.execute("ESSinkTest")
  }
}
