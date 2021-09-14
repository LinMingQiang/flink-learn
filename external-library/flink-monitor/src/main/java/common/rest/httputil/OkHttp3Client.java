package common.rest.httputil;

import okhttp3.*;
import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Map;

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
     */
    public static String getWithParameter(String url, Map<String,String> para) throws IOException {
        OkHttpClient okHttpClient = new OkHttpClient();
        FormBody.Builder formBody = new FormBody.Builder();
        para.forEach((x,y)->{
            formBody.add(x, y);
        });
        Request request = new Request
                .Builder()
                .url(url)
                .method("GET", formBody.build())
                .build();
        Call call = okHttpClient.newCall(request);
        Response response = call.execute();
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


    public static String post(String url) throws IOException {
        RequestBody formBody = new FormBody.Builder()
                .add("username", "test")
                .add("password", "test")
                .build();

        Request request = new Request.Builder()
                .url(url)
                .post(formBody)
                .build();

        Call call = asynokHttpClient.newCall(request);
        Response response = call.execute();
        String str = IOUtils.toString(new BufferedInputStream(response.body().byteStream()));
        return str;
    }

    public static String postJson(String url, String json) throws IOException {
        RequestBody body = RequestBody.create(
                MediaType.parse("application/json"), json);

        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .build();

        Call call = asynokHttpClient.newCall(request);
        Response response = call.execute();
        String str = IOUtils.toString(new BufferedInputStream(response.body().byteStream()));
        return str;
    }

}
