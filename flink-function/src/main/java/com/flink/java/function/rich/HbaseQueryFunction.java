package com.flink.java.function.rich;

import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.List;

public abstract class HbaseQueryFunction<IN, OUT> implements Serializable {
    public abstract List<OUT> transResult(List<IN> res);


}
