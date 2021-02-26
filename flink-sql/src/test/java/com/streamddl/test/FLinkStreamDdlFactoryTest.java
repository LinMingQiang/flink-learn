package com.streamddl.test;

import com.ddlsql.DDLSourceSQLManager;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.table.api.TableResult;
import org.junit.Test;

public class FLinkStreamDdlFactoryTest extends FlinkJavaStreamTableTestBase {


    @Test
    public void writetoJdbcTest() {
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test",
                        "test",
                        "test",
                        "json"));
        System.out.println(DDLSourceSQLManager.createFromMysql("mysqltest"));
        tableEnv.executeSql(DDLSourceSQLManager.createFromMysql("mysqltest"));

        TableResult re = tableEnv.executeSql("insert into mysqltest select msg,count(1) cnt from test group by msg");
        re.print();
    }

    @Test
    public void mongoTest() {
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test;test2",
                        "test",
                        "test",
                        "json"));

        System.out.println(DDLSourceSQLManager.createCustomMongoSink("mongotest"));
        tableEnv.executeSql(DDLSourceSQLManager.createCustomMongoSink("mongotest"));
        TableResult re = tableEnv.executeSql("insert into mongotest" +
                " select " +
                "'id' as id," +
                "msg," +
                "count(1) uv " +
                "from test" +
                " group by" +
                " msg," +
                " TUMBLE(proctime, INTERVAL '10' SECOND)");
//        TableResult re = tableEnv.executeSql("insert into mongotest select msg as id,msg,count(1) uv from test3 group by msg");
        re.print();
    }
}
