package com.flink.common.rest.httputil;

import okhttp3.*;

import java.io.IOException;

public class OkHttp3Client {

    private  static OkHttpClient asynokHttpClient = null;
    /**
     * @param url
     */
    public static String get(String url) throws IOException {
        System.out.println(url);
        OkHttpClient okHttpClient = new OkHttpClient();
        Request request = new Request
                .Builder()
                .url(url)
                .method("GET", null)
                .build();
        Call call = okHttpClient.newCall(request);
        Response response = call.execute();
        String str = response.body().string();
        System.out.println(str);
        return str;
    }


    /**
     * @param url
     * @param callback
     */
    public static void asynGet(String url, Callback callback) {
        if(asynokHttpClient == null)
            asynokHttpClient = new OkHttpClient();
        Request request = new Request
                .Builder()
                .url(url)
                .method("GET", null)
                .build();
        Call call = asynokHttpClient.newCall(request);
        call.enqueue(callback);
        call.cancel();
    }
    public static void closeAsyn(){
    }

}
