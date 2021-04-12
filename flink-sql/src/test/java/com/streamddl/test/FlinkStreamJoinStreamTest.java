package com.streamddl.test;

import com.ddlsql.DDLSourceSQLManager;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.types.Row;
import org.junit.Test;

/**
 * 主要用来验证 双流Join。 一个是实时输入，一个是一天一更新
 * API 写
 * 1：广播流
 * API 写
 * 2：实时两个流 （FlinkCoreOperatorEntry）（流2 作为维表存在state。用ontime来触发。wtm不影响输出）
 * API 写
 * 3：外部表 （异步Join， 时态表（lookuptable））
 */
public class FlinkStreamJoinStreamTest  extends FlinkJavaStreamTableTestBase {
    /**
     * 可用于双流join。假设 Test2为维表（永久保存状态）
     *
     * 如果在process里面要用 registerEventimeTimer 的话：
     * 双流connect 必须都有 watermark，否则一个产生不了watermark，不会触发 registerEventimeTimer
     *
     * 输入a: a1 a2 a3 ，a流的wtm = a3 。但是因为是双流 ，所以wtm取最小的 = b = min
     * 再输入b : b2 b3 ， b流的wtm = b3 。  最后的wtm = min (a3, b3) = (a3 - 10s)
     * 这个时候才触发 a的过期，a1,a2 ，如果已经超过10s了的话
     * 后面再输入其他的，就去 最小的那个。每次输入，各自流都会更新自己的wtm，然后再跟另一个比较取最小
     *
     * @throws Exception
     */
    @Test
    public void connectJoinTest(){


    }


    // 这个就是时态表了
    @Test
    public void lookupTableJoinTest() throws Exception {
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test",
                        "test",
                        "test",
                        "json"));
        tableEnv.executeSql(DDLSourceSQLManager.createHbaseLookupSource("hbaselookup"));
        tableEnv.toAppendStream(tableEnv.sqlQuery("select * from test t1 JOIN" +
                " hbaselookup FOR SYSTEM_TIME AS OF t1.proctime as t2 ON t1.msg = t2.word" +
                ""), Row.class).print();
        streamEnv.execute();
    }


    @Test
    public void others(){
        // 广播流
        // 广播文件
        // 异步io
    }
}
