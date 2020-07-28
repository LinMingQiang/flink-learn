package com.flink.func.test;

import com.flink.common.java.pojo.KafkaTopicOffsetMsgPoJo;
import com.flink.common.java.pojo.WordCountPoJo;
import com.flink.common.java.processfunc.HbaseQueryProcessFunction;
import com.flink.common.java.test.FlinkJavaStreamTableTestBase;
import com.flink.common.java.util.AbstractHbaseQueryFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FlinkFunctionTest extends FlinkJavaStreamTableTestBase {


    /**
     * 维表join ，hbase
     *
     * @throws Exception
     */
    @Test
    public void testHbaseJoin() throws Exception {
        OutputTag<Tuple2<String, KafkaTopicOffsetMsgPoJo>> queryEmpt = new OutputTag<Tuple2<String, KafkaTopicOffsetMsgPoJo>>("queryEmpt") {
        };
        Table a = tableEnv.fromDataStream(
                streamEnv.addSource(getKafkaSource("test", "localhost:9092", "latest"))
                , "topic,offset,msg");
        // 可以转stream之后再转换。pojo可以直接对应上Row
        SingleOutputStreamOperator<Tuple2<String, KafkaTopicOffsetMsgPoJo>> ds = tableEnv.toAppendStream(a, KafkaTopicOffsetMsgPoJo.class)
                .map((MapFunction<KafkaTopicOffsetMsgPoJo, Tuple2<String, KafkaTopicOffsetMsgPoJo>>) value -> new Tuple2<>(value.msg, (value)))
                .returns(new TupleTypeInfo(Types.STRING, TypeInformation.of(KafkaTopicOffsetMsgPoJo.class)));
        SingleOutputStreamOperator t = ds.keyBy(x -> x.f0)
                .process(new HbaseQueryProcessFunction(
                        new AbstractHbaseQueryFunction<KafkaTopicOffsetMsgPoJo, WordCountPoJo>() {
                            @Override
                            public List<Tuple2<Result, Tuple2<String, KafkaTopicOffsetMsgPoJo>>> queryHbase(org.apache.hadoop.hbase.client.Table t, List<Tuple2<String, KafkaTopicOffsetMsgPoJo>> d) throws IOException {
                                List ret = new ArrayList<Tuple2<Result, Tuple2<String, KafkaTopicOffsetMsgPoJo>>>();
                                for (int i = 0; i < d.size(); i++) {
                                    Tuple2<String, KafkaTopicOffsetMsgPoJo> rr = d.get(i);
                                    Result s = null;
                                    if(rr.f1.msg.equals("1")){
                                        s = new Result();
                                    }
                                    ret.add(new Tuple2(s, d.get(i)));
                                }
                                return ret;
                            }

                            @Override
                            public void transResult(Tuple2<Result, Tuple2<String, KafkaTopicOffsetMsgPoJo>> res, List<WordCountPoJo> re) {
                                if (res.f0 == null) {
                                    // 未查到
                                   // re.add(new WordCountPoJo(res.f1.f1.msg, 1L));
                                } else {
                                    re.add(new WordCountPoJo(res.f1.f1.msg, 1L));
                                }
                            }
                        },
                        "tablename",
                        100,
                        new TupleTypeInfo(Types.STRING, TypeInformation.of(KafkaTopicOffsetMsgPoJo.class)), queryEmpt))
                .returns(TypeInformation.of(WordCountPoJo.class))
                .uid("uid")
                .name("name");

        DataStream<Tuple2<String, KafkaTopicOffsetMsgPoJo>> ss = t.getSideOutput(queryEmpt);
        ss.map(x -> "no join : " + x.f1).print();


        tableEnv.createTemporaryView("wcstream", t);
        tableEnv.toRetractStream(
                tableEnv.sqlQuery("select word,sum(num) num from wcstream group by word"),
                Row.class)
                .filter(x -> x.f0)
                .print();
        tableEnv.execute("");

    }
}
