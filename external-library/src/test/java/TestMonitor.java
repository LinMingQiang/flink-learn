import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.flink.common.rest.httputil.OkHttp3Client;
import com.flink.common.yarn.api.YarnClientHandler;
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


    @Test
    public void testYarnMonitor() throws IOException, YarnException {
        YarnClientHandler yarnclient = YarnClientHandler.getInstance(null);
        List<ApplicationReport> r = yarnclient.getApplications("RUNNING");
        r.forEach(x -> {
            x.getApplicationId().toString();
        });
    }
}
