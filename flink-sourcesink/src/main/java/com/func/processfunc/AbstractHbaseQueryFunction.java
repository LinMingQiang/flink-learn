package com.func.processfunc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractHbaseQueryFunction<IN, OUT> implements Serializable {

    public abstract String getRowkey(IN input);
    /**
     * 查询结果转化
     * @param res
     * @return
     */
    public void transResult(List<Tuple2<Result, IN>> res, List<OUT> result){};

    public abstract void transResult(Tuple2<Result, IN> res, List<OUT> result);


    /**
     * 查询逻辑
     * @param t
     * @return
     * @throws IOException
     */
    public List<Tuple2<Result, IN>> queryHbase(Table t, List<IN> input) throws Exception {
        List ret = new ArrayList<Tuple2<Result, IN>>();
        Result[] r = null;
        if (t == null) {
            r = new Result[input.size()];
        } else {
            List keys = new ArrayList<Get>();
            for (int i = 0; i < input.size(); i++) {
                keys.add(new Get(getRowkey(input.get(i)).getBytes()));
            }
            r = t.get(keys);
        }
        for (int i = 0; i < input.size(); i++) {
            ret.add(new Tuple2(r[i], input.get(i)));
        }
        return ret;
    }


}
