package com.func.depfun.tablesink;

import com.func.richfunc.ElasticSinkFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

@Deprecated
public class ElasticsearchRetractStreamTableSink implements RetractStreamTableSink<Row> {
    private String[] fieldNames;
    private DataType[] fieldTypes;
    private TableSchema ts = null;

    public ElasticsearchRetractStreamTableSink(String[] fieldNames, DataType[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(getTableSchema().getFieldTypes(),getTableSchema().getFieldNames());
    }

    @Override
    public TableSchema getTableSchema() {
        if(ts == null)
            this.ts = new TableSchema.Builder().fields(fieldNames, fieldTypes).build();
        return ts;    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] strings, TypeInformation<?>[] typeInformations) {
        return this;
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        return dataStream
                .filter(x -> x.f0)
                .map(x -> x.f1)
                .addSink(new ElasticSinkFunction(fieldNames, getTableSchema().getFieldTypes(),
                        1000,
                        10))
                .name(this.getClass().getSimpleName());
    }
}
