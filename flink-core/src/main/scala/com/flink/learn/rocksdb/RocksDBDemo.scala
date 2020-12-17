//package com.flink.learn.rocksdb
//
//import org.rocksdb.RocksDB
//import org.rocksdb.Options
//import org.rocksdb.util.SizeUnit
//import org.rocksdb.CompactionStyle
//object RocksDBDemo {
//  val filep = "C:\\Users\\Administrator\\Desktop\\test.db"
//  def main(args: Array[String]): Unit = {
//    RocksDB.loadLibrary()
//    val options = new Options()
//      .setCreateIfMissing(true)
//      .setWriteBufferSize(256 * SizeUnit.MB)
//      .setMaxWriteBufferNumber(4)
//      .setMaxBackgroundCompactions(16)
//      .setCompactionStyle(CompactionStyle.UNIVERSAL)
//    val db = RocksDB.open(options, filep)
//    val f = db.put("hello".getBytes, "world".getBytes)
//    println(new String(db.get("hello".getBytes)))
//    //Thread.sleep(10000)
//    db.close()
//  }
//}
