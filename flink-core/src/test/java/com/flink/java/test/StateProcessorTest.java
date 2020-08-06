package com.flink.java.test;

import com.flink.learn.bean.WordCountGroupByKey;
import com.flink.learn.bean.WordCountPoJo;
import com.flink.learn.reader.WordCountJavaPojoKeyreader;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import com.flink.learn.trans.AccountJavaPojoKeyedStateBootstrapFunction;
import com.flink.learn.trans.AcountJavaPoJoOperatorStateBootstrap;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.Savepoint;
import org.junit.Test;
import java.io.File;

/**
 * 当 state 使用 ttlconfig的时候，readKeyedState的时候里面也要一样配
 */
public class StateProcessorTest extends FlinkJavaStreamTableTestBase {

    public static String uid = "wordcountUID";
    public static String path = "file:///Users/eminem/workspace/flink/flink-learn/checkpoint";
    public static String newPath = path + "/javatanssavepoint";
    /**
     * @throws Exception
     */
    @Test
    public void testPojoStateProcessor() throws Exception {
        String sourcePath = path + "/SocketJavaPoJoWordcountTest/202007061807/e10174e03999d77fb7655a6e9c4f64b4/chk-3";
        remove(new File(newPath.substring(7, newPath.length())));
        ExistingSavepoint existSp = Savepoint.load(bEnv, sourcePath, new RocksDBStateBackend(path));
        DataSet<WordCountPoJo> oldState1 = existSp.readKeyedState(
                uid,
                new WordCountJavaPojoKeyreader("wordcountState")
        );
        oldState1.print();
        // 对原始state做转换
        BootstrapTransformation<WordCountPoJo> transformation = OperatorTransformation
                .bootstrapWith(oldState1)
                // 必须要用 KeySelector 否则报 The generic type parameters of 'Tuple2' are missin
                .keyBy(new KeySelector<WordCountPoJo, WordCountGroupByKey>() {
                    @Override
                    public WordCountGroupByKey getKey(WordCountPoJo value) throws Exception {
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

        Savepoint.load(bEnv, newPath, new RocksDBStateBackend(path))
                .readKeyedState(
                        uid,
                        new WordCountJavaPojoKeyreader("wordcountState")
                ).print();
    }

    /**
     * @throws Exception
     */
    @Test
    public void testOpearteStateProcessor() throws Exception {
        String sourcePath = path + "/SocketJavaPoJoWordcountTest/202007071207/fe4dfe843f45d4bb61b4b0a6a4307d68/chk-2";
        remove(new File(newPath.substring(7, newPath.length())));
        ExistingSavepoint existSp = Savepoint.load(bEnv, sourcePath, new RocksDBStateBackend(path));
        // read src key state
        DataSet<WordCountPoJo> oldState1 = existSp.readListState(
                "wordcountsink",
                "opearatorstate",
                Types.POJO(WordCountPoJo.class));
        oldState1.print();
        // trans
        existSp
                .removeOperator("wordcountsink")
                .withOperator("wordcountsink", OperatorTransformation
                        .bootstrapWith(oldState1)
                        .transform(new AcountJavaPoJoOperatorStateBootstrap()))
                .write(newPath);
        bEnv.execute("jel");
        // read new key state
        Savepoint.load(bEnv, newPath, new RocksDBStateBackend(path))
                .readListState(
                        "wordcountsink",
                        "opearatorstate",
                        Types.POJO(WordCountPoJo.class)).print();
    }


    /**
     * 同时修改两个state。单独改的话互不影响
     * @throws Exception
     */
    @Test
    public void testKeyAndOperatorState() throws Exception {
        String sourcePath = path + "/SocketJavaPoJoWordcountTest/202007061807/e10174e03999d77fb7655a6e9c4f64b4/chk-3";
        remove(new File(newPath.substring(7, newPath.length())));
        ExistingSavepoint existSp = Savepoint.load(bEnv, sourcePath, new RocksDBStateBackend(path));
        // operator state
        DataSet<WordCountPoJo> oldState1 = existSp.readListState(
                "wordcountsink",
                "opearatorstate",
                Types.POJO(WordCountPoJo.class));
        oldState1.print();
        // key state
        DataSet<WordCountPoJo> keysState = existSp.readKeyedState(
                uid,
                new WordCountJavaPojoKeyreader("wordcountState")
        );
        keysState.print();

        // key state trans
        BootstrapTransformation<WordCountPoJo> keystateTransformation = OperatorTransformation
                .bootstrapWith(oldState1)
                // 必须要用 KeySelector 否则报 The generic type parameters of 'Tuple2' are missin
                .keyBy(new KeySelector<WordCountPoJo, WordCountGroupByKey>() {
                    @Override
                    public WordCountGroupByKey getKey(WordCountPoJo value) throws Exception {
                        WordCountGroupByKey k = new WordCountGroupByKey();
                        k.setKey(value.word);
                        return k;
                    }
                })// 确认状态的key
                .transform(new AccountJavaPojoKeyedStateBootstrapFunction()); // 对数据做修改
        // operator state trans
        BootstrapTransformation<WordCountPoJo> transformation = OperatorTransformation
                .bootstrapWith(oldState1)
                .transform(new AcountJavaPoJoOperatorStateBootstrap());
        // write new state
        existSp
                .removeOperator("wordcountsink")
                .withOperator("wordcountsink", transformation)
                .removeOperator(uid)
                .withOperator(uid, keystateTransformation)
                .write(newPath);
        bEnv.execute("jel");

        // read new sstate
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        Savepoint.load(bEnv, newPath, new RocksDBStateBackend(path))
                .readListState(
                        "wordcountsink",
                        "opearatorstate",
                        Types.POJO(WordCountPoJo.class)).print();

        Savepoint.load(bEnv, newPath, new RocksDBStateBackend(path))
                .readKeyedState(
                        uid,
                        new WordCountJavaPojoKeyreader("wordcountState")
                ).print();
    }
//    @Test
//    public void testTuple2StateProcessor() throws Exception {
//        remove(new File(newPath.substring(7, newPath.length())));
//        ExistingSavepoint existSp = Savepoint.load(bEnv, sourcePath, new RocksDBStateBackend(path));
//        DataSet<WordCountPoJo> oldState1 = existSp.readKeyedState(
//                uid,
//                new WordCountJavaTuple2Keyreader("wordcountState")
//        );
//        oldState1.print();
//        // 对原始state做转换
//        BootstrapTransformation<WordCountPoJo> transformation = OperatorTransformation
//                .bootstrapWith(oldState1)
//                // 必须要用 KeySelector 否则报 The generic type parameters of 'Tuple2' are missin
//                // https://ci.apache.org/projects/flink/flink-docs-release-1.10/zh/dev/java_lambdas.html
//                .keyBy(new KeySelector<WordCountPoJo, Tuple2<String, String>>() {
//                    @Override
//                    public Tuple2<String, String> getKey(WordCountPoJo value) throws Exception {
//                        return new Tuple2<String, String>(value.word, value.word);
//                    }
//                })// 确认状态的key
//                .transform(new AccountJavaTuple2KeyedStateBootstrapFunction()); // 对数据做修改
//        //转换后的数据写入新的savepoint path
//        existSp
//                .removeOperator(uid)
//                .withOperator(uid, transformation)
//                .write(newPath);
//        bEnv.execute("jel");
//
//        ExistingSavepoint existSp2 = Savepoint.load(bEnv, newPath, new RocksDBStateBackend(path));
//        existSp2.readKeyedState(
//                uid,
//                new WordCountJavaTuple2Keyreader("wordcountState")
//        ).print();
//    }


    /**
     * @param dir
     */
    public static void remove(File dir) {
        if (dir.exists()) {
            File files[] = dir.listFiles();
            for (int i = 0; i < files.length; i++) {
                if (files[i].isDirectory()) {
                    remove(files[i]);
                } else {
                    files[i].delete();
                }
            }
            //删除目录
            dir.delete();
        }
    }
}
