package com.flink.test;

import com.flink.learn.bean.TranWordCountPoJo;
import com.flink.learn.bean.WordCountGroupByKey;
import com.flink.learn.reader.WordCountJavaPojoKeyreader;
import com.flink.learn.trans.AccountJavaPojoKeyedStateBootstrapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

public class StateProcessorTest  extends AbstractTestBase implements Serializable {
    public static String uid = "wordcountUID";
    public static String path = "file:///Users/eminem/workspace/flink/flink-learn/checkpoint";
    public static String sourcePath  = path + "/SocketJavaPoJoWordcountTest/202007030907/91a28ba7c994f39149b5c8c7124da599/chk-7";
    public static String newPath = path + "/javatanssavepoint";
    public static ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();

    @Test
    public void testPojoStateProcessor() throws Exception {
        ExistingSavepoint existSp = Savepoint.load(bEnv, sourcePath , new RocksDBStateBackend(path));
        DataSet<TranWordCountPoJo> oldState1 =  existSp.readKeyedState(
                uid,
                new WordCountJavaPojoKeyreader("wordcountState")
        );
        oldState1.print();
        // 对原始state做转换
        BootstrapTransformation<TranWordCountPoJo> transformation = OperatorTransformation
                .bootstrapWith(oldState1)
                // 必须要用 KeySelector 否则报 The generic type parameters of 'Tuple2' are missin
                .keyBy(new KeySelector<TranWordCountPoJo, WordCountGroupByKey>() {
                    @Override
                    public WordCountGroupByKey getKey(TranWordCountPoJo value) throws Exception {
                        WordCountGroupByKey k = new WordCountGroupByKey();
                        k.setKey(value.word);
                        return k;
                    }
                })// 确认状态的key
                .transform(new AccountJavaPojoKeyedStateBootstrapFunction()); // 对数据做修改
        //转换后的数据写入新的savepoint path
        existSp
                .removeOperator(uid)
                .withOperator(uid, transformation)
                .write(newPath);
        bEnv.execute("jel");

        ExistingSavepoint existSp2 = Savepoint.load(bEnv, newPath , new RocksDBStateBackend(path));
        existSp2.readKeyedState(
                uid,
                new WordCountJavaPojoKeyreader("wordcountState")
        ).print();
    }
}
