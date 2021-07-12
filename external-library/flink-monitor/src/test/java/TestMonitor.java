import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import common.java.bean.ApplicationInfo;
import common.java.bean.FLinkJobsCheckpointInfo;
import common.java.bean.FlinkJobsExceptionInfo;
import common.java.bean.FlinkJobsInfo;
import common.rest.httputil.OkHttp3Client;
import common.yarn.api.YarnClientHandler;
import common.yarn.api.YarnRestFulClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.*;
public class TestMonitor {




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
        List<ApplicationInfo> r = yarnclient.getApplications(null, null);
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
        Map<String, List<FlinkJobsInfo>> m = yarnclient.getFlinkAllJobs("RUNNING");
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
        List<FlinkJobsInfo> flinkJobs = yarnclient.getFlinkJobsOverview(appid);
        String jobId = flinkJobs.get(0).jid;
        FlinkJobsExceptionInfo exceptionInfo = yarnclient.getFlinkJobExceptions(appid, jobId);
        System.out.println(exceptionInfo);
    }

    @Test
    public void testFlinkJobsCheckpoint() throws IOException, YarnException {
        YarnRestFulClient yarnclient = YarnRestFulClient.getInstance("http://10-21-129-141-jhdxyjd.mob.local:10880");
        List<ApplicationInfo> r = yarnclient.getApplications("RUNNING", "flink");
        String appid = r.get(0).id;
        List<FlinkJobsInfo> flinkJobs = yarnclient.getFlinkJobsOverview(appid);
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
    public void testFlinkTaskmanager() throws IOException, YarnException {
        YarnRestFulClient yarnclient = YarnRestFulClient.getInstance("http://10-21-129-141-jhdxyjd.mob.local:10880");
        List<ApplicationInfo> r = yarnclient.getApplications("RUNNING", "flink");
        String appid = r.get(0).id;
        yarnclient.getFlinkJobTasks(appid).forEach(x -> {
            System.out.println(x);
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
        List<FlinkJobsInfo> flinkJobs = yarnclient.getFlinkJobsOverview("application_1622709261031_0177");
        FlinkJobsInfo jobinfo = flinkJobs.get(0);
        System.out.println(yarnclient.getFlinkJobJidInfo("application_1622709261031_0177", jobinfo.jid));

    }


    /**
     * 某个顶点（Operator）的信息，可以拿到某个顶点的输入和输出
     * @throws IOException
     * @throws YarnException
     */
    @Test
    public void testFLINK_JOBS_JID_VERTICES_INFO() throws IOException, YarnException {
        YarnRestFulClient yarnclient = YarnRestFulClient.getInstance("http://10-21-129-141-jhdxyjd.mob.local:10880");
        List<FlinkJobsInfo> flinkJobs = yarnclient.getFlinkJobsOverview("application_1622709261031_0177");
        FlinkJobsInfo jobinfo = flinkJobs.get(0);
        System.out.println(yarnclient.getFlinkJobJidVerticesInfo("application_1622709261031_0177",
                jobinfo.jid,
                "d618a97df21bbd4bb61c79cdeca965b4"));

    }

    // tm的内存分配和使用信息，监控内存如果快超了预警
    // http://10-21-129-141-jhdxyjd.mob.local:10880/proxy/application_1622709261031_0177/taskmanagers/container_e2671_1622709261031_0177_01_000011

}
