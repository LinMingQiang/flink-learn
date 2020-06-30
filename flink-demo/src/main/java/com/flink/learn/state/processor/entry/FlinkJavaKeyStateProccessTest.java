package com.flink.learn.state.processor.entry;

import com.flink.learn.bean.TranWordCountPoJo;
import com.flink.learn.reader.TranWordCountPoJoKeyreader;
import com.flink.learn.reader.WordCountPoJoKeyreader;
import com.flink.learn.trans.AccountJavaKeyedStateBootstrapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.Savepoint;

import java.io.IOException;

public class FlinkJavaKeyStateProccessTest {
    public static String uid = "wordcountUID";
    public static String path = "file:///Users/eminem/workspace/flink/flink-learn/checkpoint";
    public static String sourcePath  = path + "/SocketJavaPoJoWordcountTest/202006301706/bf677b47d89cb2b0302ab1fec6552445/chk-1";
    public static String newPath = path + "/tanssavepoint";
    public static ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();

    public static void main(String[] args) throws Exception {
//	  ExistingSavepoint existSp = Savepoint.load(bEnv, sourcePath , new RocksDBStateBackend(path));
//	 readKeyState(existSp, uid).print();

//		ExistingSavepoint existSp = Savepoint.load(bEnv, sourcePath , new RocksDBStateBackend(path));
//		transKeystateAndWritebak(existSp, newPath);

		 ExistingSavepoint existSp = Savepoint.load(bEnv, newPath , new RocksDBStateBackend(path));
		 readTransKeyState(existSp, uid).print();


	}


	/**
	 * 读取历史状态
	 * @param existSp
	 * @param uid
	 * @return
	 * @throws IOException
	 */
	public static DataSet<TranWordCountPoJo> readKeyState(ExistingSavepoint existSp, String uid) throws IOException {
		return existSp.readKeyedState(
			uid,
			new WordCountPoJoKeyreader("wordcountState")
		);
	}
	/**
	 * @param uid
	 * @return
	 * @throws IOException
	 */
	public static DataSet<TranWordCountPoJo> readTransKeyState(ExistingSavepoint existSp, String uid) throws IOException {

		return existSp.readKeyedState(
				uid,
				new TranWordCountPoJoKeyreader("wordcountState")
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
        DataSet<TranWordCountPoJo> oldState1 = readKeyState(existSp, "wordcountUID");
        oldState1.print();
        // 对原始state做转换
        BootstrapTransformation<TranWordCountPoJo> transformation = OperatorTransformation
                .bootstrapWith(oldState1)
                .keyBy(x -> x.word) // 确认状态的key
                .transform(new AccountJavaKeyedStateBootstrapFunction()); // 对数据做修改
//转换后的数据写入新的savepoint path
		existSp
			    .removeOperator(uid)
                .withOperator(uid, transformation)
                .write(newPath);
		bEnv.execute("jel");
    }
}
