package com.flink.calcite.sql.sqlnode;

import org.apache.calcite.sql.SqlNode;
import org.apache.flink.sql.parser.ddl.SqlWatermark;

import javax.annotation.Nullable;

public class EmitQueryContext {
    @Nullable
    public SqlNode query;

    public SqlNode emit;

}
