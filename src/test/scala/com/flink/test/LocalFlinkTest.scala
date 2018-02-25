package com.flink.test
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import java.util.Properties
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.util.serialization.TypeInformationKeyValueSerializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.ExecutionConfig
import java.io.Serializable
import org.apache.hadoop.hbase.client.Put
import com.flink.comom.dbUtil.FlinkHbaseFactory
import org.apache.hadoop.hbase.client.Get
import com.flink.common.entry.TopicMessageDeserialize

object LocalFlinkTest {
  def main(args: Array[String]): Unit = {
   val pro = new Properties();  
        pro.put("bootstrap.servers", BROKER);  
        pro.put("zookeeper.connect", KAFKA_ZOOKEEPER);  
        pro.put("group.id", "test");
        pro.put("auto.commit.enable", "true")
        pro.put("auto.commit.interval.ms", "60000");
        
    val topicMsgSchame= new TopicMessageDeserialize()//自定义
    val kafkasource=new FlinkKafkaConsumer08[(String,String)](TOPIC.split(",").toList,topicMsgSchame ,pro)
    //kafkasource.setStartFromLatest()//默认是从上次消费
    
    val env=StreamExecutionEnvironment.createRemoteEnvironment("localhost", 6123, "F:\\workspace\\flink\\flink-kafka\\target\\flink-kafka-0.0.1-SNAPSHOT.jar")
    //env.enableCheckpointing(60000)//更新offsets。每60s提交一次
    val sourceStream = env.addSource(kafkasource)
    
    sourceStream.map{x=>
     x._1 match{case "smartadsdeliverylog"=>
     val datas=x._2.split(",")
     val statdate = datas(0).substring(0, 10) //日期
     val hour = datas(0).substring(11, 13) //hour
     (statdate+","+datas(14)+","+hour,(1,0))
     case "smartadsclicklog"=>
     val datas=x._2.split(",")
     val statdate = datas(0).substring(0, 10) //日期
     val hour = datas(0).substring(11, 13) //hour
     (statdate+","+datas(2)+","+hour,(0,1))
     }
     }
    .keyBy(0)
    .addSink{x=>
      println(x)
    }
    env.execute()
    
  }
}