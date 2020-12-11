package com.flink.learn.sql.func;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class StrSplitTableFunction extends TableFunction<Tuple2<String, String>> {
    public StrSplitTableFunction(String splitRegex) {
        this.splitRegex = splitRegex;
    }

    String splitRegex = ",";

    public void eval(String str) {
        String[] r = str.split(splitRegex);
        collect(new Tuple2<>(r[0], r[1]));
    }
}
