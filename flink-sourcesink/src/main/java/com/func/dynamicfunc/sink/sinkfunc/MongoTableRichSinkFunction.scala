package com.func.dynamicfunc.sink.sinkfunc

import com.factory.dynamicfactory.sink.MongoDynamicTableSinkFactory
import com.flink.common.dbutil.MongoDBFactory
import com.mongodb.MongoClient
import com.mongodb.client.{MongoCollection, MongoDatabase}
import org.apache.flink.configuration.{Configuration, ReadableConfig}
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.{BigIntType, DoubleType, RowType, VarCharType}
import org.apache.flink.table.types.logical.RowType.RowField
import org.apache.flink.types.RowKind
import org.bson.Document

import java.util
import java.util.List
import scala.collection.JavaConverters._
import scala.collection.mutable

class MongoTableRichSinkFunction extends RichSinkFunction[RowData] {

  @transient var mongoClient: MongoClient = _
  var db: MongoDatabase = _
  var collection: MongoCollection[Document] = _
  var converter: DynamicTableSink.DataStructureConverter = null
  var options: ReadableConfig = null;
  var shcema: mutable.Buffer[RowType.RowField] = null;

  def this(converter: DynamicTableSink.DataStructureConverter,
           options: ReadableConfig,
           shcema: DataType) {
    this()
    this.converter = converter;
    this.options = options;
    this.shcema = shcema.getLogicalType.asInstanceOf[RowType].getFields.asScala;
  }

  override def open(parameters: Configuration): Unit = {
    mongoClient = MongoDBFactory.getMongoDBConn(
      options.get(MongoDynamicTableSinkFactory.MONGO_URL),
      options.get(MongoDynamicTableSinkFactory.MONGO_USER),
      options.get(MongoDynamicTableSinkFactory.MONGO_PASSW), // 固定admin
      "admin"
    )
    db = mongoClient.getDatabase(
      options.get(MongoDynamicTableSinkFactory.MONGO_DB))
    collection = db.getCollection(
      options.get(MongoDynamicTableSinkFactory.MONGO_COLLECTION))
  }

  override def close(): Unit = {
    mongoClient.close()
  }
  override def invoke(value: RowData, context: Context): Unit = {
    value.getRowKind match {
      case RowKind.INSERT =>
        val doc = row2document(value)
        MongoDBFactory.bulkWrite(collection, Seq(doc))
      case RowKind.UPDATE_BEFORE =>
      case RowKind.UPDATE_AFTER =>
        val doc = row2document(value)
        MongoDBFactory.bulkWrite(collection, Seq(doc))
      case _ =>
    }
  }

  private def row2document(row: RowData): Document = {
    val insertdoc = new Document()
    val resetDoc = new Document()
    val reDoc = new Document()
    for (i <- 0 to shcema.size - 1) {
      shcema(i).getType match {
          // varchar的统一用insert
        case v: VarCharType =>
          if(shcema(i).getName.equals("id")){
            reDoc.append(shcema(i).getName, row.getString(i).toString)
          } else {
            insertdoc.append(shcema(i).getName, row.getString(i).toString)
          }
          // int类型的统一用 set
        case b: BigIntType =>
          resetDoc.append(shcema(i).getName, row.getLong(i))
        case c: RowType =>
          val filesd = c.getFields
          val rd = row.getRow(i, filesd.size())
         for(i <- 0 until filesd.size()) {
           filesd.get(i).getType match{
             case c: BigIntType =>
               resetDoc.append(filesd.get(i).getName, rd.getLong(i))
             case d: DoubleType =>
               resetDoc.append(filesd.get(i).getName, rd.getDouble(i))
             case _ =>
           }
         }
        case _ =>
      }
    }
    reDoc.append("$setOnInsert", insertdoc)
    reDoc.append("$set", resetDoc)
    reDoc
  }

}
