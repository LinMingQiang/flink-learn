package com.flink.common.rest.httputil;

import okhttp3.*;
import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

public class OkHttp3Client {

    private  static OkHttpClient asynokHttpClient = null;
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
