package com.flink.learn.sql.func;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

public class TimestampYearHour extends ScalarFunction {
    @DataTypeHint("ROW<d STRING ,m STRING, h STRING>")
    public Row eval(Long tsamp) {
        String formatstr = DateFormatUtils.format(tsamp, "yyyy-MM-dd HH");
        return Row.of(formatstr.substring(0, 10), formatstr.substring(0, 7), formatstr.substring(11, 13));
    }
}
