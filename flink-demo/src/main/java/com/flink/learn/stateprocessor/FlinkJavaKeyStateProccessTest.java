package com.flink.learn.stateprocessor;

import com.flink.learn.bean.TranWordCountPoJo;
import com.flink.learn.bean.WordCountGroupByKey;
import com.flink.learn.reader.WordCountPojoKeyreader;
import com.flink.learn.reader.WordCountSringKeyreader;
import com.flink.learn.reader.WordCountTuple2Keyreader;
import com.flink.learn.trans.AccountJavaKeyedStateBootstrapFunction;
import com.flink.learn.trans.AccountJavaPojoKeyedStateBootstrapFunction;
import com.flink.learn.trans.AccountJavaTuple2KeyedStateBootstrapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.state.api.*;

import java.io.IOException;

/**
 * scala 的 case class 无法存储后再读出来，因为序列号不一致
 * 但是 java pojo放在scala代码里面也不行。。。（怀疑scala的序列化问题，建议还是用java写state processor，同时不用 cass class）
 * state processor例子都正常执行，修改后重新跑没问题。
 * java代码和scala分两个source包写，所以打包的时候需要maven-jar-plugin
 */
public class FlinkJavaKeyStateProccessTest {
    public static String uid = "wordcountUID";
    public static String path = "file:///Users/eminem/workspace/flink/flink-learn/checkpoint";
    public static String sourcePath  = path + "/SocketJavaPoJoWordcountTest/202007030907/91a28ba7c994f39149b5c8c7124da599/chk-7";
    public static String newPath = path + "/javatanssavepoint";
    public static ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();

    public static void main(String[] args) throws Exception {
     ExistingSavepoint existSp = Savepoint.load(bEnv, newPath , new RocksDBStateBackend(path));

        // readTuple2KeyState(existSp, uid).print();
		// transTuple2KeystateAndWritebak(existSp,  newPath);

		//readKeyState(existSp, uid).print();
		// transKeystateAndWritebak(existSp, newPath);


		 // readPojoKeyState(existSp, uid).print();
		// transPojoKeystateAndWritebak(existSp, newPath);
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
			new WordCountSringKeyreader("wordcountState")
		);
	}

	/**
	 * 读取历史状态
	 * @param existSp
	 * @param uid
	 * @return
	 * @throws IOException
	 */
	public static DataSet<TranWordCountPoJo> readPojoKeyState(ExistingSavepoint existSp, String uid) throws IOException {
		return existSp.readKeyedState(
				uid,
				new WordCountPojoKeyreader("wordcountState")
		);
	}
	/**
	 * 读取历史状态
	 * @param existSp
	 * @param uid
	 * @return
	 * @throws IOException
	 */
	public static DataSet<TranWordCountPoJo> readTuple2KeyState(ExistingSavepoint existSp, String uid) throws IOException {
		return existSp.readKeyedState(
				uid,
				new WordCountTuple2Keyreader("wordcountState")
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

	/**
	 *
	 * @param existSp
	 * @param newPath
	 * @throws Exception
	 */
	public static void transTuple2KeystateAndWritebak(ExistingSavepoint existSp, String newPath) throws Exception {
		// 读取原始state数据
		DataSet<TranWordCountPoJo> oldState1 = readTuple2KeyState(existSp, "wordcountUID");
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
	}


	/**
	 *
	 * @param existSp
	 * @param newPath
	 * @throws Exception
	 */
	public static void transPojoKeystateAndWritebak(ExistingSavepoint existSp, String newPath) throws Exception {
		// 读取原始state数据
		DataSet<TranWordCountPoJo> oldState1 = readPojoKeyState(existSp, "wordcountUID");
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
	}
}
