package com.flink.common.manager

import org.apache.flink.table.descriptors.Kafka

object TableSourceConnectorManager {

  /**
    *
    * @param broker
    * @param topic
    * @param groupid
    * @param from
    * @return
    */
  def kafkaConnector(broker: String,
                     topic: String,
                     groupid: String,
                     from: String): Kafka = {
    val kafkaConnector = new Kafka()
      .version("0.10")
      .topic(topic)
      .property("bootstrap.servers", broker)
      .property("connector.properties.bootstrap.servers", broker)
      .property("group.id", groupid)
      .property("zookeeper.connect", "localhost:2181")
    from match {
      case "consum"   => kafkaConnector.startFromGroupOffsets()
      case "latest"   => kafkaConnector.startFromLatest()
      case "earliest" => kafkaConnector.startFromEarliest()
      case _          => kafkaConnector.startFromLatest()
    }
  }

}
