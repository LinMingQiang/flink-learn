package com.flink.learn.sql.func;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

@FunctionHint(output = @DataTypeHint("ROW<v STRING>"))
public class StrSplitToMultipleRowTableFunction extends TableFunction<Row> {
    public StrSplitToMultipleRowTableFunction(String splitRegex) {
        this.splitRegex = splitRegex;
    }
    String splitRegex = ",";
    public void eval(String... keys) {
        String[] r = keys[0].split(splitRegex);
        for (String s : r) {
            collect(Row.of(s));
        }

    }
}
