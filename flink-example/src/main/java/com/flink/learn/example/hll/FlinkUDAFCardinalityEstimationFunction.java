package com.flink.learn.example.hll;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * Flink UDF of Hyperloglog (HLL)
 * @author alice.zhu
 */
public class FlinkUDAFCardinalityEstimationFunction extends AggregateFunction<Long, VipHyperLogLog> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkUDAFCardinalityEstimationFunction.class);

    private static final int NUMBER_OF_BUCKETS = 4096;

    @Override
    public VipHyperLogLog createAccumulator() {
        int log2m = VipHyperLogLog.log2m(0.05);
        return new VipHyperLogLog(log2m, new RegisterSet(1 << log2m));
    }

    @Override
    public TypeInformation<Long> getResultType() {
        LOG.info("-- getResultType: ");
        return TypeInformation.of(Long.class);
    }

    @Override
    public TypeInformation<VipHyperLogLog> getAccumulatorType() {
        LOG.info("-- getAccumulatorType: ");
        return TypeInformation.of(VipHyperLogLog.class);
    }

    @Override
    public Long getValue(VipHyperLogLog acc) {
        if(acc == null){
            return 0L;
        }
        long v = acc.cardinality();
        return v;
    }

    public void accumulate(VipHyperLogLog acc,  String input) {

        acc.offer(input);
    }


    /**
     *  acc 这个是个空的,把其他分区的数据merge进来，
     * @param acc
     * @param it
     * @throws CardinalityMergeException
     */
    public void merge(VipHyperLogLog acc, Iterable<VipHyperLogLog> it) throws CardinalityMergeException {
        Iterator<VipHyperLogLog> iter = it.iterator();
        while (iter.hasNext()) {
            VipHyperLogLog a = iter.next();
            if(a != null) {
                acc.addAll(a);
            }
        }
    }

    public void resetAccumulator(VipHyperLogLog acc) {

    }

}
