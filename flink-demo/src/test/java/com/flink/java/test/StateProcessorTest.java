package com.flink.java.test;

import com.flink.learn.bean.TranWordCountPoJo;
import com.flink.learn.bean.WordCountGroupByKey;
import com.flink.learn.reader.WordCountJavaPojoKeyreader;
import com.flink.learn.reader.WordCountJavaTuple2Keyreader;
import com.flink.learn.trans.AccountJavaPojoKeyedStateBootstrapFunction;
import com.flink.learn.trans.AccountJavaTuple2KeyedStateBootstrapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.Test;

import java.io.File;
import java.io.Serializable;

public class StateProcessorTest  extends AbstractTestBase implements Serializable {
    public static String uid = "wordcountUID";
    public static String path = "file:///Users/eminem/workspace/flink/flink-learn/checkpoint";
    public static String sourcePath  = path + "/SocketJavaPoJoWordcountTest/202007031107/41e13c6240a3a6faa09d743bd1c8cf4c/chk-1";
    public static String newPath = path + "/javatanssavepoint";
    public static ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();


    @Test
    public void testPojoStateProcessor() throws Exception {
        remove(new File(newPath.substring(7, newPath.length())));
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

    @Test
    public void testTuple2StateProcessor() throws Exception {
        remove(new File(newPath.substring(7, newPath.length())));
        ExistingSavepoint existSp = Savepoint.load(bEnv, sourcePath , new RocksDBStateBackend(path));
        DataSet<TranWordCountPoJo> oldState1 =  existSp.readKeyedState(
                uid,
                new WordCountJavaTuple2Keyreader("wordcountState")
        );
        oldState1.print();
        // 对原始state做转换
        BootstrapTransformation<TranWordCountPoJo> transformation = OperatorTransformation
                .bootstrapWith(oldState1)
                // 必须要用 KeySelector 否则报 The generic type parameters of 'Tuple2' are missin
                .keyBy(new KeySelector<TranWordCountPoJo, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(TranWordCountPoJo value) throws Exception {
                        return new Tuple2<String, String>(value.word, value.word);
                    }
                })// 确认状态的key
                .transform(new AccountJavaTuple2KeyedStateBootstrapFunction()); // 对数据做修改
        //转换后的数据写入新的savepoint path
        existSp
                .removeOperator(uid)
                .withOperator(uid, transformation)
                .write(newPath);
        bEnv.execute("jel");

        ExistingSavepoint existSp2 = Savepoint.load(bEnv, newPath , new RocksDBStateBackend(path));
        existSp2.readKeyedState(
                uid,
                new WordCountJavaTuple2Keyreader("wordcountState")
        ).print();
    }


    public static void remove(File dir) {
        File files[] = dir.listFiles();
        for (int i = 0; i < files.length; i++) {
            if(files[i].isDirectory()) {
                remove(files[i]);
            }else {
                files[i].delete();
            }
        }
        //删除目录
        dir.delete();
    }
}
