package com.flink.learn.sql.func;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableFunction;

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
