package com.test;

import org.apache.hudi.cli.commands.SparkMain;
import org.junit.Test;

public class HudiClientAndSparkTest {

	@Test
	public void testRollBackToSvp() throws Exception {
		String[] args = new String[7];
		args[0] = "ROLLBACK_TO_SAVEPOINT";
		args[1] = "local";
		args[2] = "1G";
		args[3] = "20220218110009847";
		args[4] = "hdfs://localhost:9000/tmp/hudi/hive.test.hudi_svp_test";
		SparkMain.main(args);
	}
}
