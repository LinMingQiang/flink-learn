package com.flink.common.entry

import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.BasicTypeInfo

class TopicMessageDeserialize extends KeyedDeserializationSchema[(String,String)]{
 override def deserialize(x:Array[Byte],x2:Array[Byte], topic:String ,partition:Int , offset:Long )={
   (topic,new String(x2))
 }
 override def isEndOfStream(nextElement:(String,String))={
   false
 }
 override def getProducedType()={
  BasicTypeInfo.getInfoFor(classOf[Tuple2[String,String]])
 }
}