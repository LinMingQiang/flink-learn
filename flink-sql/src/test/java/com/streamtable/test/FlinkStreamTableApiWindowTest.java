package com.streamtable.test;

import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.table.api.*;
import org.junit.Test;

import static org.apache.flink.table.api.Expressions.*;

public class FlinkStreamTableApiWindowTest extends FlinkJavaStreamTableTestBase {

    @Test
    public void testGroupWindow() throws Exception {
        // {"rowtime":"2020-01-01 00:00:00","msg":"hello"}
        // {"rowtime":"2020-01-01 00:02:00","msg":"hello"}
        // 定时触发
//        tableEnv.getConfig().getConfiguration().setBoolean("table.exec.emit.early-fire.enabled", true);
//        tableEnv.getConfig().getConfiguration().setLong("table.exec.emit.early-fire.delay", 5000L);
        Table orders = getStreamTable(kafkaDataSource, $("topic"),
                $("offset"),
                $("rowtime"),
                $("msg"),
                $("ts").rowtime());
        // lit(10) 表示一个常数
        Table result = orders
                .filter(
                        and(
                                $("topic").isNotNull(),
                                $("msg").isNotNull(),
                                $("ts").isNotNull()
                        )
                )
                .select($("msg").lowerCase().as("msg"), $("topic"), $("ts"));

        Table r1 =  result
                .window(Tumble.over(lit(50).seconds()).on($("ts")).as("hourlyWindow"))
                .groupBy($("hourlyWindow"), $("topic"), $("msg"))
                .select($("topic"),
                        $("hourlyWindow").end().as("secWindow"),
                        $("msg")
                                .count().as("cnt"));

        Table r2 = result
                .window(Tumble.over(lit(10).seconds()).on($("ts")).as("hourlyWindow"))
                .groupBy($("hourlyWindow"), $("topic"), $("msg"))
                .select($("topic"),
                        $("hourlyWindow").end().as("secWindow"),
                        $("msg")
                                .count().as("cnt"));
        printlnStringTable(r1);
        printlnStringTable(r2);
        streamEnv.execute("");

    }

    @Test
    public void testOverWindow() throws Exception {
        // UNBOUNDED_ROW 和 UNBOUNDED_RANGE结果不一样
        // {"ts":13,"msg":"hello"}  {"ts":35,"msg":"hello"} {"ts":35,"msg":"hello"} {"ts":65,"msg":"hello"}
        Table orders = getStreamTable(kafkaDataSource, $("topic"),
                $("offset"),
                $("date"),
                $("msg"),
                $("ts").rowtime());

        Table res = orders
                .window(Over
                        .partitionBy($("msg"))
                        .orderBy($("ts")) // 必须是时间，rowtime或者proctime
                        // UNBOUNDED_ROW 每次窗口触发就统计前面所有的。
                        // lit(1).minutes() 往前计算1分钟的数据
                        .preceding(UNBOUNDED_RANGE) // 往前3个元素。 也就是每3个元素一个窗口计算，
                        .as("w")
                )
                .select($("msg"),
                        $("offset").sum().over($("w")).as("over_offset_sum"),
                        $("offset").min().over($("w")).as("over_offset_min")); // aggregate over the over window w
//        res.addColumns($("pt").proctime());
//        res.addColumns($("rt").rowtime()); // 必须要转流，然后再这个才行，table->stream->table才能重新加上timearr
        printlnStringTable(res);
        streamEnv.execute("");

    }


}
