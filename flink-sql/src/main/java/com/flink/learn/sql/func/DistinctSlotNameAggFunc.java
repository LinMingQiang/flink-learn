package com.flink.learn.sql.func;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

@DataTypeHint("ROW<slot_name STRING, app_id STRING, app_name STRING>")
public class DistinctSlotNameAggFunc extends AggregateFunction<Row, Tuple4<Long,String,String,String>> {

    @Override
    public Row getValue(Tuple4<Long,String,String, String> accumulator) {
        return Row.of(accumulator.f0, accumulator.f1, accumulator.f2);
    }

    @Override
    public Tuple4<Long,String,String, String> createAccumulator() {
        return new Tuple4(0, "-1", "-1", "-1");
    }


    public void accumulate(Tuple4<Long,String,String, String> acc,
                           Long time,
                           String slot_name,
                           String app_id,
                           String app_name
                           ) {
        if( time > acc.f0 ) {
            acc.f0 = time;
            acc.f1 = slot_name;
            acc.f2 = app_id;
            acc.f3 = app_name;
        }
    }

}
