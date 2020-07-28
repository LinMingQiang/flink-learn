package com.flink.pro.func

import com.flink.pro.common.CaseClassUtil.ReportInfo
import com.flink.pro.common.StreamPropertiesUtil
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.common.xcontent.{XContentBuilder, XContentFactory}
object ElasticDocAssembleUtil {
  /**
    * 组装卖家报表
    * @param groupKey
    * @param setIndicators
    * @return
    */
  def assembleReportDoc(groupKey: Array[String])(
      setIndicators: XContentBuilder => XContentBuilder): (XContentBuilder) = {
    val Array(day,
              hour,
              buyerId,
              seller_id,
              adslot_id,
              contry,
              provinces,
              city,
              policyId) = groupKey
    val createDoc = XContentFactory.jsonBuilder().startObject()
    createDoc.field("day", day)
    createDoc.field("hour", hour)
    createDoc.field("adplatformId", buyerId)
    createDoc.field("appId", seller_id)
    createDoc.field("adslotId", adslot_id)
    createDoc.field("policyId", policyId)
    createDoc.field("countryCode", contry)
    createDoc.field("provinceCode", provinces)
    createDoc.field("cityCode", city)
    setIndicators(createDoc).endObject() // 设置指标值
    (createDoc)
  }

  /**
    *
    * @param value
    * @return
    */
  def getUpsertIndex(value: ReportInfo): UpdateRequest = {
    val setIndicators = (xf: XContentBuilder) => {
      xf.field("income", value.rv.income)
        .field("clickNum", value.rv.clickNum)
        .field("impressionNum", value.rv.impressionNum) // 写es的时候bid = req
        .field("bidReqNum", value.rv.bidReqNum)
        .field("fillNum", value.rv.fillNum)
    }
    val updateDoc = XContentFactory.jsonBuilder().startObject()
    setIndicators(updateDoc).endObject() // 设置指标值
    val createDoc = value.tablename match {
      case StreamPropertiesUtil.SELLER_INDEX =>
        assembleReportDoc(value.groupKey)(setIndicators)
    }
    val index = new UpdateRequest(value.tablename,
                                  StreamPropertiesUtil.INDEX_TYPE,
                                  value.keybyKey) // es：6.x版本有 INDEX_TYPE
      .upsert(createDoc)
      .retryOnConflict(3)
      .doc(updateDoc)
    index
  }

}
