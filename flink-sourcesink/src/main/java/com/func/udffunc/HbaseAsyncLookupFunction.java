package com.func.udffunc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.types.Row;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

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

    public void eval(CompletableFuture<Collection<Row>> future, Object... params) {}
}
