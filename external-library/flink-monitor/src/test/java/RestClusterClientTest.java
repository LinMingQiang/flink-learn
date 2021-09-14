import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;

import java.io.File;

public class RestClusterClientTest {
    /**
     * 使用代码的方式提交
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.setString(JobManagerOptions.ADDRESS, "localhost");
        config.setInteger(RestOptions.RETRY_MAX_ATTEMPTS, 10);
        config.setLong(RestOptions.RETRY_DELAY, 0);
        config.setInteger(RestOptions.PORT, 8081);
        RestClusterClient client = new RestClusterClient<>(
                config,
                StandaloneClusterId.getInstance());

        String jar = "/Users/eminem/workspace/flink/flink-learn/dist/lib/flink-core-1.13.0.jar";
        PackagedProgram program = PackagedProgram.newBuilder()
                .setJarFile(new File(jar))
                .setEntryPointClassName("com.flink.learn.entry.FlinkCoreOperatorEntry")
                .setArguments(args)
                .build();
        JobGraph jobGraph = PackagedProgramUtils.createJobGraph(program, config, 1, false);

        client.submitJob(jobGraph).get();

    }
}
