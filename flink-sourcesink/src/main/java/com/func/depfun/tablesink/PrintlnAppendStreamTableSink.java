package com.func.depfun.tablesink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

@Deprecated
public class PrintlnAppendStreamTableSink implements AppendStreamTableSink<Row>, BatchTableSink<Row> {
    private String[] fieldNames;
    private DataType[] fieldTypes;

    public PrintlnAppendStreamTableSink(String[] fieldNames, DataType[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    public PrintlnAppendStreamTableSink() {

    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return getTableSchema().getFieldTypes();
    }

    @Override
    public TableSchema getTableSchema() {
        return new TableSchema.Builder().fields(fieldNames, fieldTypes).build();
    }
    public DataType getConsumedDataType() {
        return getTableSchema().toRowDataType();
    }

//    @Override
//    public void emitDataSet(DataSet<Row> dataSet) {
//        List<Row> elements = null;
//        try {
//            elements = dataSet.collect();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        Iterator var2 = elements.iterator();
//
//        while (var2.hasNext()) {
//            Row e = (Row) var2.next();
//            System.out.println(e);
//        }
//        try {
//            dataSet.print();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//    }
    @Override
    public DataStreamSink<Row> consumeDataStream(DataStream<Row> dataStream) {
        return dataStream.addSink(new SinkFunction<Row>() {
            @Override
            public void invoke(Row value, Context context) throws Exception {
                System.out.println("》》》 " + (value).toString());
            }
        }).name(this.getClass().getSimpleName());
    }

    @Override
    public TableSink configure(String[] strings, TypeInformation<?>[] typeInformations) {
        return this;
    }
    @Override
    public DataSink<?> consumeDataSet(DataSet<Row> dataSet) {
        return null;
    }
}
