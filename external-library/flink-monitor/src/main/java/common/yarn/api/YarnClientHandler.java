package common.yarn.api;

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

public class YarnClientHandler {
    private YarnClient yarnClient =null;
    /**
     * 需要再classpath里面放入yarn-site.xml
     * @return
     */
    private void init(Map<String, String> properties) {
        System.out.println(properties);
        YarnConfiguration conf = new YarnConfiguration();
        if(properties != null && !properties.isEmpty()){
            properties.forEach((k, v) -> {
                conf.set(k, v);
            });
        }
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
        yarnClient.init(conf);
    }

    /**
     *  获取application的信息
     * @param states
     * @return
     * @throws IOException
     * @throws YarnException
     */
    public List<ApplicationReport> getApplications(String states) throws IOException, YarnException {
        if(states == null || states.isEmpty()) {
            return yarnClient.getApplications();
        } else {
            switch (states.toUpperCase()){
                case "RUNNING" : return yarnClient.getApplications(EnumSet.of(YarnApplicationState.RUNNING));
                default :   return yarnClient.getApplications();
            }
        }
    }


    private static class YarnClientHandlerInstans{
        private static YarnClientHandler INSTANCE = null;
        private static void init(Map<String, String> properties){
            INSTANCE = new YarnClientHandler();
            INSTANCE.init(properties);
        }

    }

    /**
     * 单例模式-。-
     * @param properties
     * @return
     */
    public static YarnClientHandler getInstance(Map<String, String> properties) {
        if(YarnClientHandlerInstans.INSTANCE == null){
            YarnClientHandlerInstans.init(properties);
        }
        return YarnClientHandlerInstans.INSTANCE;
    }
}
