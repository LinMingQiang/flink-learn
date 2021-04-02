package com.streamddl.test;

import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.Test;

public class FlinkJiraBugTest extends FlinkJavaStreamTableTestBase {


    /**
     * 如果
     * @throws Exception
     */
    @Test
    public void udftest() throws Exception {
        tableEnv.createTemporaryView("test" ,tableEnv.fromDataStream(streamEnv.fromElements("a", "b", "c", "d", "e"), "msg"));
        tableEnv.registerFunction("sample", new SampleFunction());
        Table tm = tableEnv.sqlQuery("select msg, sample() as c from test");
        tableEnv.createTemporaryView("tmp", tm);
//        System.out.println(r.explain());

        tableEnv.toAppendStream(tm, Row.class).print();

        tableEnv.toAppendStream(tableEnv.sqlQuery("select * from tmp where c > 5"), Row.class).print();
//        System.out.println(streamEnv.getExecutionPlan());
        streamEnv.execute();
    }


    @Test
    public void iftest2() throws Exception {
        tableEnv.createTemporaryView("test" ,tableEnv.fromDataStream(streamEnv.fromElements("a", "", ""), "msg"));
        Table tm = tableEnv.sqlQuery("" +
                "SELECT msg,\n" +
                "IF(msg = '' OR msg IS NULL, 'N', 'Y') as ss\n" +
                "FROM test\n" +
                "\n");
        tableEnv.toAppendStream(tm, Row.class).print();
        streamEnv.execute();
    }
    // UDF函数
    public class SampleFunction extends ScalarFunction {
        public int eval() {
           int a = (int) (Math.random() * 10);
            System.out.println("************************" + a );
            return a;
        }
    }
}
