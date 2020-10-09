package com.aiguigu.apitest

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
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

    // 打开检查点
    env.enableCheckpointing(1000)
    //    env.getCheckpointConfig.setCheckpointingMode()
    //    env.getCheckpointConfig.setCheckpointTimeout(60 * 1000)
    // 最多允许多少个checkpoint在进行
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)

    // 没两个checkpoint间最小的间隔时间, 防止没有时间做真正的计算
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(60000)

    // 是否倾向于用checkpoint做故障恢复
    // 如果有一个比checkpoint更近的savepoint 那么用哪个?
    // 不配也罢
    env.getCheckpointConfig.setPreferCheckpointForRecovery(true)
    // 做checkpoint失败允许的次数
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(2)

    // 任务重启策略
    // 1. failureRateRestart
    // 2. fallbackRestart
    // 3. fixDelayRestart
    // 4. noRestart
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000))
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(5), Time.seconds(10)))
  }
}
