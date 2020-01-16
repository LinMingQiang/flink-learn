package com.flink.learn.state.processor.entry;

import com.flink.learn.bean.WordCountPoJo;
import com.flink.learn.reader.WordCountKeyreader;
import com.flink.learn.reader.WordCountPoJoKeyreader;
import com.flink.learn.trans.AccountJavaKeyedStateBootstrapFunction;
import com.flink.learn.trans.AccountKeyedStateBootstrapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.Savepoint;
import java.io.IOException;
import java.util.Date;

public class FlinkJavaKeyStateProccessTest {
    public static String uid = "wordcountUID";
    public static String path = "file:///Users/eminem/workspace/flink/flink-learn/checkpoint";
    public static String savp = "file:///Users/eminem/workspace/flink/flink-learn/savepoint";
    public static String sourcePath  = savp + "/savepoint-840b62-50c952b06168";
    public static String newPath = savp + "/" + (new Date().getTime());
    public static String date = "/1579167297854";
    public static ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();

    public static void main(String[] args) throws Exception {
		 ExistingSavepoint existSp = Savepoint.load(bEnv, savp + date , new RocksDBStateBackend(path));
		readKeyStatePoJo(existSp, "wordcountUID").print();


//		ExistingSavepoint existSp = Savepoint.load(bEnv, sourcePath, new RocksDBStateBackend(path));
//		transKeystateAndWritebak(existSp, newPath);
//		bEnv.execute("jel");

	}

    /**
     * @param existSp
     * @param uid
     * @return
     * @throws IOException
     */
    public static DataSet<WordCountPoJo> readKeyState(ExistingSavepoint existSp, String uid) throws IOException {
        return existSp.readKeyedState(
			uid,
                new WordCountKeyreader("wordcountState")
        );
    }


	/**
	 * 读取历史状态
	 * @param existSp
	 * @param uid
	 * @return
	 * @throws IOException
	 */
	public static DataSet<WordCountPoJo> readKeyStatePoJo(ExistingSavepoint existSp, String uid) throws IOException {
		return existSp.readKeyedState(
			uid,
			new WordCountPoJoKeyreader("wordcountState")
		);
	}

    /**
     *
     * @param existSp
     * @param newPath
     * @throws Exception
     */
    public static void transKeystateAndWritebak(ExistingSavepoint existSp, String newPath) throws Exception {
    	// 读取原始state数据
        DataSet<WordCountPoJo> oldState1 = FlinkJavaKeyStateProccessTest.readKeyState(existSp, "wordcountUID");
        // oldState1.print();
        // 对原始state做转换
        BootstrapTransformation<WordCountPoJo> transformation = OperatorTransformation
                .bootstrapWith(oldState1)
                .keyBy(x -> x.w) // 确认状态的key
                .transform(new AccountJavaKeyedStateBootstrapFunction()); // 对数据做修改
//转换后的数据写入新的savepoint path
		existSp
			    .removeOperator("wordcountUID")
                .withOperator("wordcountUID", transformation)
                .write(newPath);

    }
}
