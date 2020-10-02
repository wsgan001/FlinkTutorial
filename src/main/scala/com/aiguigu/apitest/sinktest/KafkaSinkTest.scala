package com.aiguigu.apitest.sinktest
import com.aiguigu.apitest.SensorReading
import com.aiguigu.apitest.TransformTest.getClass
import com.aiguigu.apitest.sinktest.FileSink.getClass
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputPath = getClass.getResource("/sensor.txt").getPath
    val inputStream = env.readTextFile(inputPath)
    val dataStream = inputStream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble).toString
      }
    )

    // 从Kafka读取
    // TODO

    dataStream.addSink(new FlinkKafkaProducer011[String]("node01:9092","kafkasinktest", new SimpleStringSchema()))

    env.execute("KafkaSinkTest")
  }
}
