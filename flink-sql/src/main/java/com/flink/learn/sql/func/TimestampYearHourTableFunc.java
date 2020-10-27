package com.flink.learn.sql.func;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

@FunctionHint(output = @DataTypeHint("ROW<d STRING ,m STRING, h STRING>"))
public class TimestampYearHourTableFunc extends TableFunction<Row> {
    public void eval(Long tsamp) {
        String formatstr = DateFormatUtils.format(tsamp, "yyyy-MM-dd HH");
        collect(Row.of(formatstr.substring(0, 10), formatstr.substring(0, 7), formatstr.substring(11, 13)));
    }
}
