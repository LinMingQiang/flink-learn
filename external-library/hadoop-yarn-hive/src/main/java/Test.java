import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.net.URISyntaxException;

public class Test {
	/**
	 * 连不上本地的docker，是因为9000端口没映射出来
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		System.out.println(System.getenv("HADOOP_HOME"));
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
//		conf.set("");
		FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
		;
		System.out.println(fs.mkdirs(new Path("/tmp")));
	}
}
