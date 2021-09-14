package common.yarn.api;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import common.java.bean.*;
import common.rest.httputil.OkHttp3Client;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static common.java.bean.RestfulUrlParameter.*;

public class YarnRestFulClient {
    // yarn的rest前缀
    public  String YARN_REST_PREFIX = "/ws/v1/cluster";
    // flink的rest前缀
    private String FLINK_REST_PREFIX = null;

    private YarnRestFulClient() {
    }

    /**
     * 需要再classpath里面放入yarn-site.xml
     *
     * @return
     */
    private void init(String yarnUrl) {
        YARN_REST_PREFIX = yarnUrl + "/ws/v1/cluster";
        FLINK_REST_PREFIX = yarnUrl + "/proxy";
    }

    private static class YarnRestFulClientInstans {
        private static YarnRestFulClient INSTANCE = null;

        private static void init(String yarnUrl) {
            INSTANCE = new YarnRestFulClient();
            INSTANCE.init(yarnUrl);
        }

    }

    /**
     * 获取application的信息
     *
     * @param states
     * @return
     * @throws IOException
     * @throws YarnException
     */
    public List<ApplicationInfo> getApplications(String states, String applicationType) throws IOException, YarnException {
        List<ApplicationInfo> apps = new ArrayList<>();
        JSONObject json = null;
        if (states == null || states.isEmpty()) {
            json = JSON.parseObject(OkHttp3Client.get(YARN_REST_PREFIX + YARN_APPS));
        } else {
            if ("RUNNING".equals(states.toUpperCase())) {
                json = JSON.parseObject(OkHttp3Client.get(YARN_REST_PREFIX + YARN_APPS_STATE + states));
            } else
                json = JSON.parseObject(OkHttp3Client.get(YARN_REST_PREFIX + YARN_APPS));
        }
        json.getJSONObject("apps").getJSONArray("app").forEach(x -> {
            ApplicationInfo tmp = JSON.parseObject(x.toString(), ApplicationInfo.class);
            if (applicationType != null
                    && !applicationType.isEmpty()) {
                if (tmp.applicationType.toLowerCase().contains(applicationType.toLowerCase()))
                    apps.add(tmp);
            } else {
                apps.add(tmp);
            }

        });
        return apps;
    }


    /**
     * @param appid
     * @return
     * @throws IOException
     */
    public List<FlinkApplicationJobsInfo> getFlinkJobsOverview(String appid) throws IOException {
//        System.out.println(FLINK_REST_PREFIX + "/" + appid + FLINK_STREAM_JOBS_OVERVIEW);
        List<FlinkApplicationJobsInfo> re = new ArrayList<>();
        JSON.parseObject(OkHttp3Client.get(FLINK_REST_PREFIX + "/" + appid + FLINK_STREAM_JOBS_OVERVIEW))
                .getJSONArray("jobs").forEach(x -> {
            re.add(JSON.parseObject(x.toString(), FlinkApplicationJobsInfo.class));
        });

        return re;
    }

    /**
     * @param appid
     * @param jid
     * @return
     * @throws IOException
     */
    public FlinkJobsExceptionInfo getFlinkJobExceptions(String appid, String jid) throws IOException {
        return JSON.parseObject(OkHttp3Client.get(
                FLINK_JOBS_EXCEPTION(FLINK_REST_PREFIX, appid, jid)), FlinkJobsExceptionInfo.class);

    }

    /**
     * 获取flink作业的ckp
     *
     * @param appid
     * @param jid
     * @return
     * @throws IOException
     */
    public FLinkJobsCheckpointInfo getFlinkJobCheckpoint(String appid, String jid) throws IOException {
        return JSON.parseObject(OkHttp3Client.get(
                FLINK_JOBS_CHECKPOINT(FLINK_REST_PREFIX, appid, jid)), FLinkJobsCheckpointInfo.class);
    }


    public List<FlinkTaskManagerInfo> getFlinkJobAllTasksManagersInfo(String appid) throws IOException {
//        System.out.println(FLINK_ALL_TASK_MANAGER(FLINK_REST_PREFIX, appid));
        JSONObject json = JSON.parseObject(OkHttp3Client.get(FLINK_ALL_TASK_MANAGER(FLINK_REST_PREFIX, appid)));
        List<FlinkTaskManagerInfo> re = new ArrayList<>();
        for (Object taskmanagersInfo : json.getJSONArray("taskmanagers")) {
            re.add(JSON.parseObject(taskmanagersInfo.toString(), FlinkTaskManagerInfo.class));
        }
        return re;
    }



    public FlinkJobJidInfo getFlinkJobJidInfo(String appid, String jid) throws IOException {
//        System.out.println(FLINK_JOBS_JID_INFO(FLINK_REST_PREFIX, appid, jid));
        JSONObject json = JSON.parseObject(OkHttp3Client.get(FLINK_JOBS_JID_INFO(FLINK_REST_PREFIX, appid, jid)));
        return JSON.parseObject(json.toString(), FlinkJobJidInfo.class);
    }

    /**
     *
     * @param appid
     * @param tmId
     * @return
     * @throws IOException
     */
    public FlinkJobTaskManagerInfo getFLinkTaskManagerInfo(String appid, String tmId) throws IOException {
        JSONObject json = JSON.parseObject(OkHttp3Client.get(FLINK_TASK_MANAGER(FLINK_REST_PREFIX,appid, tmId)));
        return JSON.parseObject(json.toString(), FlinkJobTaskManagerInfo.class);
    }
    /**
     * 获取全部的flink作业
     *
     * @param states
     * @return Map<String, List < FlinkApplicationJobsInfo>> ： appid->jobid
     * @throws IOException
     * @throws YarnException
     */
    public Map<String, List<FlinkApplicationJobsInfo>> getFlinkAllApplicationJobsInfo(String states) throws IOException, YarnException {
        Map<String, List<FlinkApplicationJobsInfo>> re = new HashMap<>();
        this.getApplications(states, "flink").forEach(x -> {
            try {
                re.put(x.id, this.getFlinkJobsOverview(x.id));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        return re;
    }

    /**
     * 单例模式-。-
     *
     * @param yarnUrl
     * @return
     */
    public static YarnRestFulClient getInstance(String yarnUrl) {
        if (YarnRestFulClientInstans.INSTANCE == null) {
            YarnRestFulClientInstans.init(yarnUrl);
        }
        return YarnRestFulClientInstans.INSTANCE;
    }
}
