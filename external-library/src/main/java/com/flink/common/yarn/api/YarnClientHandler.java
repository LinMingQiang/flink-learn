package com.flink.common.yarn.api;

import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

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
