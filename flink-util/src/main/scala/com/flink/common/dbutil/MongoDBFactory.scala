package com.flink.common.dbutil

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.{UpdateOneModel, UpdateOptions}

import scala.collection.JavaConverters._
import com.mongodb.{MongoClient, MongoCredential, ServerAddress}
import org.bson.Document

import scala.collection.JavaConverters._

object MongoDBFactory {

  def getMongoDBConn(hostPort: String,
                     userName: String,
                     passw: String,
                     databaseName: String): MongoClient = {
    val serverAddresses = hostPort
      .split("\\s*,\\s*")
      .map(ele => {
        val hostPort = ele.split(":")
        new ServerAddress(hostPort(0), hostPort(1).toInt)
      })
      .toList
    val mongoCredential = MongoCredential.createScramSha1Credential(
      userName,
      databaseName,
      passw.toCharArray)
    new MongoClient(serverAddresses.asJava, List(mongoCredential).asJava)
  }

  /**
   *
   * @param docs
   */
   def bulkWrite(collection: MongoCollection[Document], insertAndSet: Seq[Document]): Unit = {
    val updateOptions = new UpdateOptions().upsert(true)
    val requests =
      insertAndSet.map { insertDoc =>
        val _id = insertDoc.get("id")
        insertDoc.remove("id")
        new UpdateOneModel[Document](
          new Document("_id", _id),
          insertDoc,
          updateOptions
        )
      }
    if (requests.nonEmpty) {
      collection.bulkWrite(requests.toList.asJava)
    }
  }

  //
  def main(args: Array[String]): Unit = {
    val databaseName = "test"
    val collectionName = "AppDayTest"
    val connUser = "admin"
    val connPassw = "123456"
    val host = "localhost:27017"
    val mongoClient = MongoDBFactory.getMongoDBConn(
      host,
      connUser,
      connPassw, // 固定admin
      "admin")
    val db = mongoClient.getDatabase(databaseName)
    val collection = db.getCollection(collectionName)
    val doc = new Document()
    doc.append("id", "id")
    doc.append("msg", "msg")
    doc.append("uv", 100L)

    bulkWrite(collection, Seq(doc))

//    val updateOptions = new UpdateOptions().upsert(true)
//    //获取数据库连接对象
//    //获取集合
//    //要插入的数据
//    val doc = new Document()
//      .append("appkey", "test5")
//
//    val o = new Document()
//    o.append("$setOnInsert", doc) // 当不存在就执行 $setOnInsert 和 $set
//      .append("$set", new Document("uv", 2L)) // 当存在就执行$set
//      .append("$inc", new Document("pv", 100L))
//
//    println(o)
//
//    val document = new UpdateOneModel[Document](
//      new Document("_id", "id2"),
//      o,
//      updateOptions
//    )
//
//    //插入一个文档
//    import scala.collection.JavaConverters._
//    collection.bulkWrite(List(document).asJava)
  }
}
