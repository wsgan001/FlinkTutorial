package com.aiguigu.apitest

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object StateBackendTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(new FsStateBackend("uri"))
    env.setStateBackend(new MemoryStateBackend())
    env.setStateBackend(new RocksDBStateBackend(""))
  }
}
