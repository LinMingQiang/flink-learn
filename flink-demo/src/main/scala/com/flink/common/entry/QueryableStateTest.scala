//package com.flink.common.entry
//
//import org.apache.flink.api.common.JobID
//import org.apache.flink.queryablestate.client.QueryableStateClient
//import org.apache.flink.api.common.state.ValueStateDescriptor
//import com.flink.common.bean.StatisticalIndic
//import org.apache.flink.api.common.typeinfo.BasicTypeInfo
//
//
//object QueryableStateTest {
//  def main(args: Array[String]): Unit = {
//
//    val jobId = JobID.fromHexString("793edfa93f354aa0274f759cb13ce79e")
//    val key = ""
//    val client = new QueryableStateClient("localhost", 9069);
//    val descriptor =
//      new ValueStateDescriptor[StatisticalIndic](
//        "StatisticalIndic",
//        classOf[StatisticalIndic])
////    val resultFuture =
////      client.getKvState(jobId, "StatisticalIndic", key, BasicTypeInfo.STRING_TYPE_INFO, descriptor);
//
//
//  }
//}
