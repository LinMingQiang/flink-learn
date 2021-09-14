import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import common.java.bean.*;
import common.rest.httputil.OkHttp3Client;
import common.yarn.api.YarnRestFulClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.*;
public class TestMonitor {
    public static YarnRestFulClient yarnclient;
    static {
        yarnclient = YarnRestFulClient.getInstance("http://10-21-129-141-jhdxyjd.mob.local:10880");
    }

    @Test
    public void testHandler() throws Exception {
        List<ApplicationInfo> r = yarnclient.getApplications("running", "flink");
        r.forEach(x -> {
            try {
                List<FlinkApplicationJobsInfo> jobInfos = yarnclient.getFlinkJobsOverview(x.id);
                FlinkApplicationJobsInfo jobInfo = jobInfos.get(0);
                FlinkJobsExceptionInfo jobsExcepInfo = yarnclient.getFlinkJobExceptions(x.id, jobInfo.jid);
                System.out.println(jobsExcepInfo);
                FLinkJobsCheckpointInfo ckpInfo= yarnclient.getFlinkJobCheckpoint(x.id, yarnclient.getFlinkJobsOverview(x.id).get(0).jid);
                System.out.println(ckpInfo.latest);

                // tm的内存信息
//                List<FlinkTaskManagerInfo> flinkAllTmInfos = yarnclient.getFlinkJobAllTasksManagersInfo(x.id);
//                flinkAllTmInfos.forEach(tminfo -> {
//                    try {
//                        FlinkJobTaskManagerInfo tmin = yarnclient.getFLinkTaskManagerInfo(x.id, tminfo.id);
////                        System.out.println(tmin);
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                });




            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    public void testHttp() throws IOException {
        JSONObject json = JSON.parseObject(OkHttp3Client.get("http://10-21-129-142-jhdxyjd.mob.local:10880/ws/v1/cluster/apps?state=RUNNING"));

        json.getJSONObject("apps").getJSONArray("app").forEach(x -> {
            System.out.println(JSON.parseObject(x.toString()).getString("id"));
        });
//        OkHttp3Client.asynGet("http://www.baidu.com"
//        , new Callback(){
//
//                    @Override
//                    public void onResponse(@NotNull Call call, @NotNull Response response) throws IOException {
//                        String data = response.body().string();
//                        try {
//                            Thread.sleep(1000L);
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }
//                        System.out.println(data);
//                    }
//
//                    @Override
//                    public void onFailure(@NotNull Call call, @NotNull IOException e) {
//
//                    }
//                });
//        System.out.println(">>>>>>>>");
    }

    /**
     * 获取所有app信息
     * @throws IOException
     * @throws YarnException
     */
    @Test
    public void testYarnAllappInfo() throws IOException, YarnException {
        YarnRestFulClient yarnclient = YarnRestFulClient.getInstance("http://10-21-129-141-jhdxyjd.mob.local:10880");
        List<ApplicationInfo> r = yarnclient.getApplications("running", null);
        System.out.println(r.size());

       // r.forEach(x -> System.out.println(x));

    }

    /**
     * 获取running的flink的app信息
     * @throws IOException
     * @throws YarnException
     */
    @Test
    public void testYarnMonitor() throws IOException, YarnException {
        YarnRestFulClient yarnclient = YarnRestFulClient.getInstance("http://10-21-129-141-jhdxyjd.mob.local:10880");
        List<ApplicationInfo> r = yarnclient.getApplications("RUNNING", "flink");
        r.forEach(x -> System.out.println(x));
    }

    /**
     * 获取 flink某app的job信息
     * @throws IOException
     * @throws YarnException
     */
    @Test
    public void testFlinkjobs() throws IOException, YarnException {
        YarnRestFulClient yarnclient = YarnRestFulClient.getInstance("http://10-21-129-141-jhdxyjd.mob.local:10880");
        Map<String, List<FlinkApplicationJobsInfo>> m = yarnclient.getFlinkAllApplicationJobsInfo("RUNNING");
       m.forEach( (k, v)-> System.out.println(k + "->" + v));

    }


    /**
     * 获取某flink某jid的报错信息
     * @throws IOException
     * @throws YarnException
     */
    @Test
    public void testFlinkJobException() throws IOException, YarnException {
        YarnRestFulClient yarnclient = YarnRestFulClient.getInstance("http://10-21-129-141-jhdxyjd.mob.local:10880");
        List<ApplicationInfo> r = yarnclient.getApplications("RUNNING", "flink");
        String appid = r.get(0).id;
        List<FlinkApplicationJobsInfo> flinkJobs = yarnclient.getFlinkJobsOverview(appid);
        String jobId = flinkJobs.get(0).jid;
        FlinkJobsExceptionInfo exceptionInfo = yarnclient.getFlinkJobExceptions(appid, jobId);
        System.out.println(exceptionInfo);
    }

    @Test
    public void testFlinkJobsCheckpoint() throws IOException, YarnException {
        YarnRestFulClient yarnclient = YarnRestFulClient.getInstance("http://10-21-129-141-jhdxyjd.mob.local:10880");
        List<ApplicationInfo> r = yarnclient.getApplications("RUNNING", "flink");
        String appid = r.get(0).id;
        List<FlinkApplicationJobsInfo> flinkJobs = yarnclient.getFlinkJobsOverview(appid);
        String jobId = flinkJobs.get(0).jid;
        FLinkJobsCheckpointInfo ckpinfo = yarnclient.getFlinkJobCheckpoint(appid, jobId);
        System.out.println(ckpinfo.history.size());
        ckpinfo.history.forEach(x -> System.out.println(x));

    }


    /**
     * 获取tm的信息
     * @throws IOException
     * @throws YarnException
     */
    @Test
    public void testFlinkAllTaskmanager() throws IOException, YarnException {
        YarnRestFulClient yarnclient = YarnRestFulClient.getInstance("http://10-21-129-141-jhdxyjd.mob.local:10880");
        List<ApplicationInfo> r = yarnclient.getApplications("RUNNING", "flink");
        String appid = r.get(0).id;
        yarnclient.getFlinkJobAllTasksManagersInfo(appid).forEach(x -> {
            System.out.println(x);
        });
    }

    /**
     * 获取tm的信息
     * @throws IOException
     * @throws YarnException
     */
    @Test
    public void testFlinkTaskmanager() throws IOException, YarnException {
        YarnRestFulClient yarnclient = YarnRestFulClient.getInstance("http://10-21-129-141-jhdxyjd.mob.local:10880");
        List<ApplicationInfo> r = yarnclient.getApplications("RUNNING", "flink");
        String appid = r.get(0).id;
        yarnclient.getFlinkJobAllTasksManagersInfo(appid).forEach(x -> {
            try {
                System.out.println(yarnclient.getFLinkTaskManagerInfo(appid, x.id));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }
    /**
     * 获取JID的具体信息，包括plan，source，sink等的输入输出（testFLINK_JOBS_JID_VERTICES_INFO），并发度，包含
     * @throws IOException
     * @throws YarnException
     */
    @Test
    public void testFLINK_JOBS_JID_INFO() throws IOException, YarnException {
        YarnRestFulClient yarnclient = YarnRestFulClient.getInstance("http://10-21-129-141-jhdxyjd.mob.local:10880");
        List<ApplicationInfo> r = yarnclient.getApplications("RUNNING", "flink");
        String appid = r.get(0).id;
        List<FlinkApplicationJobsInfo> flinkJobs = yarnclient.getFlinkJobsOverview(appid);
        FlinkApplicationJobsInfo jobinfo = flinkJobs.get(0);
        String jobid = jobinfo.jid;
        yarnclient.getFlinkJobJidInfo(appid, jobid).vertices.forEach(x -> {
            System.out.println(x.metrics);
        });
    }
}
