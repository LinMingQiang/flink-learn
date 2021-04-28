package com.func.udffunc;

import com.flink.common.core.FlinkLearnPropertiesUtil;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.types.Row;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class HbaseAsyncLookupFunction extends AsyncTableFunction<Row> {
    private String[] fieldNames;
    private TypeInformation[] fieldTypes;


    public HbaseAsyncLookupFunction(String[] fieldNames, TypeInformation[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

//    @Override
//    public TypeInformation<Row> getResultType() {
//        return new RowTypeInfo(fieldTypes, fieldNames);
//    }

    public void eval(CompletableFuture<Collection<Row>> future, Object... params) {

    }
}
