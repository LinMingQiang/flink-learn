package com.dbutil;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import okhttp3.*;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.ExceptExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Map;

public class OkHttp3Client {

//    private  static OkHttpClient asynokHttpClient = null;
    public static Logger log = LoggerFactory.getLogger(OkHttp3Client.class);
    /**
     * @param url
     */
    public static String get(String url) throws IOException {
        OkHttpClient okHttpClient = new OkHttpClient();
        Request request = new Request
                .Builder()
                .url(url)
                .method("GET", null)
                .build();
        Call call = okHttpClient.newCall(request);
        Response response = call.execute();
        // String str = response.body().string();
        String str = IOUtils.toString(new BufferedInputStream(response.body().byteStream()));
        return str;
    }

    /**
     * @param url
     */
    public static String postWithParameter(String url, Map<String, String> para) throws IOException {
        OkHttpClient okHttpClient = new OkHttpClient();
        FormBody.Builder formBody = new FormBody.Builder();
        para.forEach((x, y) -> {
            formBody.add(x, y);
        });
        Request request = new Request
                .Builder()
                .url(url)
                .method("POST", formBody.build())
                .build();
        Call call = okHttpClient.newCall(request);
        Response response = call.execute();
        String str = IOUtils.toString(new BufferedInputStream(response.body().byteStream()));
        return str;
    }

    //    public static void asynGet(String url, Callback callback) {
//        if(asynokHttpClient == null)
//            asynokHttpClient = new OkHttpClient();
//        Request request = new Request
//                .Builder()
//                .url(url)
//                .method("GET", null)
//                .build();
//        Call call = asynokHttpClient.newCall(request);
//        call.enqueue(callback);
//        call.cancel();
//    }
    public static void closeAsyn() {
    }


    public static String post(String url) throws IOException {
        OkHttpClient okHttpClient = new OkHttpClient();
        RequestBody formBody = new FormBody.Builder()
                .add("username", "test")
                .add("password", "test")
                .build();
        Request request = new Request.Builder()
                .url(url)
                .post(formBody)
                .build();

        Call call = okHttpClient.newCall(request);
        Response response = call.execute();
        String str = IOUtils.toString(new BufferedInputStream(response.body().byteStream()));
        return str;
    }

    public static String postJson(String url, String json) throws IOException {
        OkHttpClient okHttpClient = new OkHttpClient();
        RequestBody body = RequestBody.create(
                MediaType.parse("application/json"), json);
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .build();
        Call call = okHttpClient.newCall(request);
        Response response = call.execute();
        String str = IOUtils.toString(new BufferedInputStream(response.body().byteStream()));
        return str;
    }

    public static JSONArray getAppinfos(String url, String json) {
        try {
            String str = postJson(url, json);
            if(!str.isEmpty() && str.startsWith("{") && str.length()>5){
                return JSON.parseObject(str)
                        .getJSONObject("res")
                        .getJSONArray("item");
            }
            return null;
        } catch (Exception e) {
            log.error(e.toString());
            return null;
        }
    }

    public static JSONObject getAppinfo(String url, String appkey) throws IOException {
        try {
            OkHttpClient okHttpClient = new OkHttpClient();
            RequestBody formBody = new FormBody.Builder()
                    .add("appkey", appkey)
                    .build();
            Request request = new Request.Builder()
                    .url(url)
                    .post(formBody)
                    .build();
            Call call = okHttpClient.newCall(request);
            Response response = call.execute();
            String str = IOUtils.toString(new BufferedInputStream(response.body().byteStream()));
            if(!str.isEmpty() && str.startsWith("{") && str.contains("200")){
                return JSON.parseObject(str).getJSONObject("res").getJSONObject("item");
            } else return null;

        }catch (Exception e){
            log.error(e.toString());
            return null;
        }

    }
}
