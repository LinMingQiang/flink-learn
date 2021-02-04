package com.flink.streamtable.entry;
import com.flink.common.java.core.FlinkSourceBuilder;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.table.runtime.operators.join.temporal.BaseTwoInputStreamOperatorWithStateRetention;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class FlinkStreamTableApi extends FlinkSourceBuilder {
    public static void main(String[] args) throws Exception {
        init();
        if (args.length == 0) {
            runTemJoin();
        } else
            switch (args[0]) {
                case "runTemJoin":
                    runTemJoin();
                case "runStreamConnect":
            }
        streamEnv.execute("FlinkCoreOperatorEntry"); //程序名
    }


    public static void runTemJoin() {
        // {"ts":104,"msg":"1"}  {"ts":10,"msg":"1"}  {"ts":100,"msg":"1"}
        initJsonSource(true);
        Table orders = getStreamTable(cd1, $("topic"),
                $("offset"),
                $("date"),
                $("msg").as("o_currency"),
                $("ts").rowtime().as("o_rowtime"));

        Table ratesHistory = getStreamTable(cd2, "msg,ts.rowtime");// 提供一个汇率历史记录表静态数据集

        printlnStringTable(ratesHistory);
        printlnStringTable(orders);

        TemporalTableFunction rates = ratesHistory.createTemporalTableFunction(
                $("ts"),
                $("msg"));
        tableEnv.createTemporarySystemFunction("rates", rates);


        Table result = orders
                .joinLateral(
                        call("rates", $("o_rowtime")),
                        $("o_currency").isEqual($("msg")))
                .select($("o_rowtime"));
        printlnStringTable(result);
    }




}
