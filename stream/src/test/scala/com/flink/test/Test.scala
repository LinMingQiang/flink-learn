package com.flink.test
import com.flink.common.core.FlinkEvnBuilder
import com.flink.pro.common.StreamPropertiesUtil
import com.flink.pro.common.StreamPropertiesUtil._
import com.flink.pro.func.MysqlAssembleUtil
import com.flink.pro.rich.{AnalysisLogData, DataFormatReportInfoRichfm}
import com.flink.pro.sink.ReportMysqlSink
import com.flink.pro.state.{ReportFlatmapDistincReqID, ReportFlatmapState}
import org.apache.flink.streaming.api.scala._
import org.slf4j.LoggerFactory

/**
  * @author ${user.name}
  */
object Test {

  val _log = LoggerFactory.getLogger(Test.getClass)
  def main(args: Array[String]): Unit = {
    val proPath =
      "/Users/eminem/workspace/flink/flink-pro-demo/dist/conf/application.properties"
    StreamPropertiesUtil.init(proPath, "PRO_NAME")
    val kafkasource =
      getKafkaSource(TOPIC, BROKER, "test2")
    kafkasource.setStartFromEarliest()
    val env = FlinkEvnBuilder.buildFlinkEnv(param, CHECKPOINT_PATH, 20000) // 1 min

    env
      .addSource(kafkasource)
      .uid("kafkasource")
      .name("kafkasource")
      .flatMap(new AnalysisLogData) // 数据转化

//      .keyBy("topic", "req_id")
//      .flatMap(new ReportFlatmapDistincReqID(240))
//      .name("reqId 去重 ")
//      .uid("distinctreqid") // 去重reqid

      // 格式化成reportinfo
      .flatMap(new DataFormatReportInfoRichfm)
      .name("格式化数据 info ")
      .uid("ReportInfo")

      // 按表名和主键做sum
      .keyBy("keybyKey", "tablename")
      .flatMap { new ReportFlatmapState(240) }
      .name("数据聚合计算")
      .uid("ReportFlatmapState")
      .setParallelism(5) // 这里的并行的要跟 sink的保持一直，否则上面的keyby的结果将会重新随机分配，导致同一个key在不同的slot

      .addSink(new ReportMysqlSink(10,
        10,
        SDK_REPLACE_INSERT_SQL(MYSQL_REPORT_TBL),
        MysqlAssembleUtil.assembleRptMysqlPSt()))
      .name("数据写入mysql")
      .uid("ReportMysqlSink") // 10s钟提交一次，或者100提交一次，checkpoint的时候提交一次
      .setParallelism(5) // 这里的并行的要跟 sink的保持一直，否则上面的keyby的结果将会重新随机分配，导致同一个key在不同的slot

    env.execute("AdFlink_SDK_RTP") //程序名
  }
}
