package com.flink.java.sourcesink.test;

import com.flink.common.java.connect.PrintlnConnect;
import com.flink.common.manager.SchemaManager;
import com.flink.learn.sql.common.DDLSourceSQLManager;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import com.flink.sql.common.format.ConnectorFormatDescriptorUtils;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.descriptors.Json;
import org.junit.Test;

public class FlinkStreamCoreSourceSinkTest extends FlinkJavaStreamTableTestBase {
    /**
     * 自定义的sinkfactory
     * 百度SPI
     * 1： 需要再resources/META-INF.services/下创建一个接口名-(org.apache.flink.table.factories.TableFactory)的SPI文件（不是txt）
     * 2：在文件里面写上自己实现的类路径
     * 3：实现PrintlnAppendStreamFactory。
     */
    @Test
    public void testcustomSinkFactory() throws Exception {
        Table a = tableEnv.fromDataStream(
                streamEnv.addSource(getKafkaSource("test", "localhost:9092", "latest"))
                , "topic,offset,msg").renameColumns("offset as ll"); // offset是关键字
        tableEnv.createTemporaryView("test", a);


        tableEnv.sqlUpdate(DDLSourceSQLManager.createCustomPrintlnRetractSinkTbl("printlnSinkTbl"));
        tableEnv.sqlQuery("select topic,msg,count(1) as ll from test group by topic,msg").insertInto("printlnSinkTbl");

//        tableEnv.sqlUpdate(DDLSourceSQLManager.createCustomSinkTbl("printlnSinkTbl"));
//        tableEnv.insertInto("printlnSinkTbl", tableEnv.sqlQuery("select * from test"));

        tableEnv.execute("");
    }

    /**
     * 功能同上
     * @throws Exception
     */
    @Test
    public void testPrintlneConnect() throws Exception {
        Table a = tableEnv.fromDataStream(
                streamEnv.addSource(getKafkaSource("test", "localhost:9092", "latest"))
                , "topic,offset,msg").renameColumns("offset as ll"); // offset是关键字
        tableEnv.createTemporaryView("test", a);


//        tableEnv
//                .connect(new PrintlnConnect().property("println.prefix", "connect sink : "))
//                .inAppendMode()
//                .withFormat(ConnectorFormatDescriptorUtils.kafkaConnJsonFormat())
//                .withSchema(SchemaManager.PRINTLN_SCHEMA())
//                .createTemporaryTable("printlnSinkTbl");
//        tableEnv.insertInto("printlnSinkTbl", a.select("topic,msg,ll"));


        tableEnv
                .connect(new PrintlnConnect("printsink_retract", 1, true))
                .inRetractMode()
                .withFormat(ConnectorFormatDescriptorUtils.kafkaConnJsonFormat())
                .withSchema(SchemaManager.PRINTLN_SCHEMA())
                .createTemporaryTable("printlnSinkTbl");

        tableEnv.insertInto("printlnSinkTbl",
                tableEnv.sqlQuery("select topic,msg,count(ll) as ll from test group by topic,msg"));


        tableEnv.execute("");

    }

    @Test
    public void testHbaseConnect() {

    }
}
