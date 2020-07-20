package com.flink.pro

import org.apache.flink.api.common.state.StateTtlConfig
import org.apache.flink.api.common.time.Time

package object state {

  /**
    *
    * @return
    */
  def getStateTTLConf(timeOut: Long = 120): StateTtlConfig = {
    StateTtlConfig
      .newBuilder(Time.minutes(timeOut)) // 2个小时
      .cleanupFullSnapshot() // 创建完整快照时清理
      .cleanupInBackground()
      .cleanupInRocksdbCompactFilter() // 达到100个过期就清理？
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .build();
  }
}
