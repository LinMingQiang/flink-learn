package com.flink.learn.state.processor.entry;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.Savepoint;
import com.flink.learn.bean.CaseClassUtil.Wordcount;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;

public class FlinkJavaKeyStateProccessTest {
    public static String uid = "wordcountUID";
    public static String path = "file:///Users/eminem/workspace/flink/flink-learn/checkpoint";
    public static String savp = "file:///Users/eminem/workspace/flink/flink-learn/savepoint";
    public static String sid = "savepoint-840b62-50c952b06168";
    public static ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();

    public static void main(String[] args) throws Exception {
        ExistingSavepoint existSp = Savepoint.load(bEnv, savp + "/" + sid, new RocksDBStateBackend(path));
        // DataSet<Wordcount> df = readKeyState(existSp, uid);
       // df.print();
        transKeystateAndWritebak(existSp, savp);
    }

    /**
     * @param existSp
     * @param uid
     * @return
     * @throws IOException
     */
    public static DataSet<Wordcount> readKeyState(ExistingSavepoint existSp, String uid) throws IOException {
        return existSp.readKeyedState(
                "wordcountUID",
                new WordCountKeyreader("wordcountState")
        );
    }

    /**
     *
     * @param existSp
     * @param newPath
     * @throws Exception
     */
    public static void transKeystateAndWritebak(ExistingSavepoint existSp, String newPath) throws Exception {
        DataSet<Wordcount> oldState1 = FlinkJavaKeyStateProccessTest.readKeyState(existSp, "wordcountUID");
        DataSet<Wordcount> oldState = bEnv.fromCollection(Arrays.asList(new Wordcount("", 1L, 1L)));
        BootstrapTransformation<Wordcount> transformation = OperatorTransformation
                .bootstrapWith(oldState)
                .keyBy(x -> x.w())
                .transform(new AccountJavaKeyedStateBootstrapFunction());
        Savepoint
                // .load(bEnv, savp + "/" + sid, new RocksDBStateBackend(path))
                .create(new RocksDBStateBackend(path), 3)
                // .removeOperator("wordcountUID")
                .withOperator("wordcountUID2", transformation)
                .write(newPath);

        bEnv.execute("jel");
    }
}
