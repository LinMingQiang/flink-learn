import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.flink.common.java.bean.ApplicationInfo;
import com.flink.common.java.bean.FlinkJobsExceptionInfo;
import com.flink.common.java.bean.FlinkJobsInfo;
import com.flink.common.rest.httputil.OkHttp3Client;
import com.flink.common.yarn.api.YarnClientHandler;
import com.flink.common.yarn.api.YarnRestFulClient;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.List;
import org.junit.*;
public class TestMonitor {




    @Test
    public void testHttp() throws IOException {
        JSONObject json = JSON.parseObject(OkHttp3Client.get("http://localhost:10880/ws/v1/cluster/apps?state=RUNNING"));

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
        List<ApplicationInfo> r = yarnclient.getApplications("RUNNING", "flink");
        yarnclient.getFlinkJobs(r.get(0).id);


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
        List<FlinkJobsInfo> flinkJobs = yarnclient.getFlinkJobs(appid);
        String jobId = flinkJobs.get(0).jid;
        FlinkJobsExceptionInfo exceptionInfo = yarnclient.getFlinkJobExceptions(appid, jobId);
        System.out.println(exceptionInfo);
    }
}
