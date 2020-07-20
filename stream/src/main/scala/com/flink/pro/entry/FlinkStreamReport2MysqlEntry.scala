package com.flink.pro.entry

import com.flink.common.core.FlinkEvnBuilder
import com.flink.common.source.KafkaSourceManager
import com.flink.pro.common.StreamPropertiesUtil
import com.flink.pro.common.StreamPropertiesUtil._
import com.flink.pro.func.MysqlAssembleUtil
import com.flink.pro.rich.{AnalysisLogData, DataFormatReportInfoRichfm}
import com.flink.pro.sink.ReportMysqlSink
import com.flink.pro.state.ReportFlatmapState
import org.apache.flink.streaming.api.scala._
import org.slf4j.LoggerFactory

object FlinkStreamReport2MysqlEntry {
  val _log = LoggerFactory.getLogger(this.getClass)
  val PRO_NAME = "RPT_"

  /**
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val proPath = args(0)
    StreamPropertiesUtil.init(proPath, PRO_NAME);
    val kafkasource =
      KafkaSourceManager.getKafkaSource(TOPIC, BROKER, "latest", "test")
    val env =
      FlinkEvnBuilder.buildStreamTableEnv(param, CHECKPOINT_PATH, CHECKPOINT_INTERVAL)
    env
      .addSource(kafkasource)
      .uid("kafkasource")
      .name("Kafka 输入源")
      .flatMap(new AnalysisLogData)
      .name("格式化数据 SourceLogData")
      .flatMap(new DataFormatReportInfoRichfm)
      .name("格式化数据 ReportInfo") // 格式化成reportinfo
      .keyBy("keybyKey", "tablename") // 按表名和主键做sum
      .flatMap { new ReportFlatmapState(240) }
      .name("数据聚合计算")
      .uid("ReportFlatmapState")
      .setParallelism(5) // 这里的并行的要跟 sink的保持一直，否则上面的keyby的结果将会重新随机分配，导致同一个key在不同的slot
      .addSink(new ReportMysqlSink(1000,
                                   10,
                                   SDK_REPLACE_INSERT_SQL(MYSQL_REPORT_TBL),
                                   MysqlAssembleUtil.assembleRptMysqlPSt()))
      .name("数据写入mysql")
      .uid("ReportMysqlSink") // 10s钟提交一次，或者100提交一次，checkpoint的时候提交一次
      .setParallelism(5) // 这里的并行的要跟 sink的保持一直，否则上面的keyby的结果将会重新随机分配，导致同一个key在不同的slot

    //      .keyBy("topic", "req_id")
    //      .flatMap(new ReportFlatmapDistincReqID(240))
    //      .name("reqId 去重 ")
    //      .uid("distinctreqid") // 去重reqi
    env.execute("AdFlink_FLINK_RTP") //程序名
  }
}
