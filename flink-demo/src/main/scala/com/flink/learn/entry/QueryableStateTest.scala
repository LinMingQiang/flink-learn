package com.flink.learn.entry

import org.apache.flink.api.common.JobID
import org.apache.flink.queryablestate.client.QueryableStateClient
import org.apache.flink.api.common.state.ValueStateDescriptor
import com.flink.learn.bean.StatisticalIndic
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
object QueryableStateTest {
  def main(args: Array[String]): Unit = {
    val jobId = JobID.fromHexString("62817d5bbb14829f1fefd01588a661c8")
    val key = ""
    val client = new QueryableStateClient("localhost", 9069);
    val descriptor =
      new ValueStateDescriptor[StatisticalIndic](
        "StatisticalIndic",
        classOf[StatisticalIndic])
    val resultFuture =
      client.getKvState(jobId, "StatisticalIndic", key, BasicTypeInfo.STRING_TYPE_INFO, descriptor);


  }
}
