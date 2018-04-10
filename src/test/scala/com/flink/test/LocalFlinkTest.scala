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
    var count = 0;
    val pro = new Properties();
    pro.put("bootstrap.servers", BROKER);
    pro.put("zookeeper.connect", KAFKA_ZOOKEEPER);
    pro.put("group.id", "test");
    pro.put("auto.commit.enable", "true")
    pro.put("auto.commit.interval.ms", "60000");

    val topicMsgSchame = new TopicMessageDeserialize() //自定义
    val kafkasource = new FlinkKafkaConsumer08[(String, String)](TOPIC.split(",").toList, topicMsgSchame, pro)
    kafkasource.setStartFromLatest()//不加这个默认是从上次消费
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(10)
    env.enableCheckpointing(60000)//更新offsets。每60s提交一次
    val sourceStream = env.addSource(kafkasource)
    sourceStream.map { x =>
      println(x)
      x._1 match {
        case "smartadsdeliverylog" =>
          val datas = x._2.split(",")
          val statdate = datas(0).substring(0, 10) //日期
          val hour = datas(0).substring(11, 13) //hour
          val plan = datas(25)
          if (plan.nonEmpty) {
            WordCount(statdate + "," + plan + "," + hour, 1, 0)
          } else WordCount(null, 1, 0)
        case "smartadsclicklog" =>
          val datas = x._2.split(",")
          val statdate = datas(0).substring(0, 10) //日期
          val hour = datas(0).substring(11, 13) //hour
          val plan = datas(17)
          if (plan.nonEmpty) {
            WordCount(statdate + "," + plan + "," + hour, 0, 1)
          } else WordCount(null, 0, 1)
      }
    }
      .filter { _.key != null }
      .keyBy("key")//按key分组，可以把key相同的发往同一个slot处理
      .addSink { wc =>
        val put = new Put(wc.key.getBytes)
        val get = FlinkHbaseFactory.get("test", new Get(wc.key.getBytes), Array("pv", "cv"))
        val (npv,ncv)=if (get != null) {
          (wc.pv + get(0).toInt,wc.cv + get(1).toInt)
        }else (wc.pv,wc.cv)
        put.addColumn("info".getBytes, "pv".getBytes, npv.toString.getBytes)
        put.addColumn("info".getBytes, "cv".getBytes, ncv.toString.getBytes)
        FlinkHbaseFactory.put("test", put)
      }
    env.execute()

  }
  case class WordCount(key: String, pv: Int, cv: Int) {
    override def toString() = {
      key + "|" + pv + "|" + cv
    }
  }
}