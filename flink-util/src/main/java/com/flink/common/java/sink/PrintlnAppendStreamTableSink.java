package com.flink.common.java.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.util.Iterator;
import java.util.List;

public class PrintlnAppendStreamTableSink implements AppendStreamTableSink<Row>, BatchTableSink<Row> {
    @Override
    public void emitDataSet(DataSet<Row> dataSet) {
        List<Row> elements = null;
        try {
            elements = dataSet.collect();
        } catch (Exception e) {
            e.printStackTrace();
        }
        Iterator var2 = elements.iterator();

        while (var2.hasNext()) {
            Row e = (Row) var2.next();
            System.out.println(e);
        }


        try {
            dataSet.print();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private String[] fieldNames;
    private TypeInformation<?>[] fieldTypes;

    @Override
    public DataStreamSink<Row> consumeDataStream(DataStream<Row> dataStream) {
        emitDataStream(dataStream);
        return dataStream.addSink(new SinkFunction<Row>() {
            @Override
            public void invoke(Row value, Context context) throws Exception {
                System.out.println((value).toString());
            }
        });
    }

    @Override
    public TableSink configure(String[] strings, TypeInformation<?>[] typeInformations) {
        PrintlnAppendStreamTableSink configuredSink = new PrintlnAppendStreamTableSink();
        configuredSink.fieldNames = strings;
        configuredSink.fieldTypes = typeInformations;
        return configuredSink;
    }

    @Override
    public void emitDataStream(DataStream dataStream) {
        //输出到控制台
        //dataStream.print();
        //dataStream.addSink(new PrintSinkFunction<>());

        //打印到日志
//            dataStream.addSink(new SinkFunction() {
//                @Override
//                public void invoke(Object value, Context context) throws Exception {
//                    System.out.println(((Row)value).toString());
//                }
//            });

    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }

    @Override
    public TypeInformation<Row> getOutputType() {
        return Types.ROW_NAMED(fieldNames, fieldTypes);
    }
}
