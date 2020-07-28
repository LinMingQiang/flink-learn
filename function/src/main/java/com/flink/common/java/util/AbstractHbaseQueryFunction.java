package com.flink.common.java.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractHbaseQueryFunction<IN, OUT> implements Serializable {

    public abstract List<OUT> transResult(List<Tuple2<Result, Tuple2<String, IN>>> res);

    public List<Tuple2<Result, Tuple2<String, IN>>> queryHbase(Table t, List<Tuple2<String, IN>> d) throws IOException {
        List keys = new ArrayList<Get>();
        for (int i = 0; i < d.size(); i++) {
            keys.add(new Get(d.get(i).f0.getBytes()));
        }
        List ret = new ArrayList<Tuple2<Result, Tuple2<String, IN>>>();
        Result[] r = null;
        if (t == null) {
            r = new Result[d.size()];
        } else {
            r = t.get(keys);
        }
        for (int i = 0; i < d.size(); i++) {
            ret.add(new Tuple2(r[i], d.get(i)));
        }
        return ret;
    }

}
