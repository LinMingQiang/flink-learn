package com.streamtable.test;

import com.ddlsql.DDLSourceSQLManager;
import com.flink.common.java.pojo.KafkaTopicOffsetMsgPoJo;
import com.flink.common.java.pojo.TestRowPoJo;
import com.flink.common.java.pojo.WordCountPoJo;
import com.flink.function.common.AbstractHbaseQueryFunction;
import com.flink.function.process.HbaseQueryProcessFunction;
import com.flink.learn.sql.func.StrSplitTableFunction;
import com.flink.learn.sql.func.StrSplitToMultipleRowTableFunction;
import com.flink.learn.sql.func.TimestampYearHour;
import com.flink.learn.sql.func.TimestampYearHourTableFunc;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.*;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Test;

import java.util.List;

import static org.apache.flink.table.api.Expressions.*;

public class FlinkStreamTableApiWindowTest extends FlinkJavaStreamTableTestBase {

    @Test
    public void testGroupWindow() throws Exception {
        // {"ts":10,"msg":"hello"}  {"ts":100,"msg":"hello"}
        // 定时触发
//        tableEnv.getConfig().getConfiguration().setBoolean("table.exec.emit.early-fire.enabled", true);
//        tableEnv.getConfig().getConfiguration().setLong("table.exec.emit.early-fire.delay", 5000L);
        initJsonSource(true);
        Table orders = getStreamTable(d1, $("topic"),
                $("offset"),
                $("date"),
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
        printlnStringTable(r1.addColumns($("dd").proctime()));
        printlnStringTable(r2);
        streamEnv.execute("");

    }

    @Test
    public void testOverWindow() throws Exception {
        // UNBOUNDED_ROW 和 UNBOUNDED_RANGE结果不一样
        // {"ts":13,"msg":"hello"}  {"ts":35,"msg":"hello"} {"ts":35,"msg":"hello"} {"ts":65,"msg":"hello"}
        initJsonSource(true);
        Table orders = getStreamTable(d1, $("topic"),
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
