package com.flink.common

import java.util.Properties

import scala.collection.JavaConversions._
import com.flink.common.deserialize.TopicMessageDeserialize
import com.flink.common.param.EnvironmentalKey
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.connectors.kafka.{
  FlinkKafkaConsumer010,
  KafkaDeserializationSchema
}

package object entry extends EnvironmentalKey {

  /**
    * @desc 获取env
    * @param checkpointPath
    * @return
    */
  def getFlinkEnv(checkpointPath: String, interval: Long = 6000) = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.enableCheckpointing(interval) //更新offsets。每60s提交一次
    //超时
    //env.getCheckpointConfig.setCheckpointTimeout(5000)
    // 同一时间只允许进行一个检查点
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1);
    // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(
      ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    //env.setStateBackend(new FsStateBackend(checkpointPath))
    val rocksDBStateBackend = new RocksDBStateBackend(checkpointPath)
    rocksDBStateBackend.setDbStoragePath(checkpointPath + "/rocksdbstorage")
    env.setStateBackend(rocksDBStateBackend.asInstanceOf[StateBackend])
    env
  }

  /**
    *
    */
  def getKafkaParam(broker: String) = {
    val pro = new Properties()
    pro.put("bootstrap.servers", broker)
    // pro.put("zookeeper.connect", KAFKA_ZOOKEEPER)
    pro.put("group.id", "test")
    pro.put("auto.commit.enable", "false") //kafka 0.8-
    pro.put("enable.auto.commit", "false") //kafka 0.9+
    // pro.put("auto.commit.interval.ms", "60000")
    pro
  }

  case class KafkaMessge(topic: String, msg: String)
  case class KafkaTopicOffsetMsg(topic: String, offset: Long, msg: String)

  /**
    *
    * @param topic
    * @param broker
    * @return
    */
  def getKafkaSource[T](topic: String,
                        broker: String,
                        deserialize: KafkaDeserializationSchema[T]) = {
    new FlinkKafkaConsumer010[T](topic.split(",").toList,
                                 deserialize,
                                 getKafkaParam(broker))
  }
}
