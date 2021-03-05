package com.func.dynamicfunc.source.tablefunc;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;

public class HbaseTableFunc extends TableFunction<RowData> {
    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
    }

    public void eval(Object... keys) {
        // 还可以使用缓存技术，查到之后放缓存，先查缓存再查数据库
        // 对于批量查询，这个没有资料，
        // 这里用collect的方法返回Row类型的数据
        GenericRowData genericRowData = new GenericRowData(1);
        genericRowData.setField(0, StringData.fromString("hahahah"));
        this.collect(genericRowData);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }


}
