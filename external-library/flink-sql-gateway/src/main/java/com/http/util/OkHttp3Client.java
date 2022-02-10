package com.http.util;

import okhttp3.*;
import org.apache.commons.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class OkHttp3Client {

	private static OkHttpClient asynokHttpClient = null;

	public static void initClient(){
		if (asynokHttpClient == null) {
			asynokHttpClient =
					new OkHttpClient.Builder()
							.readTimeout(60, TimeUnit.SECONDS)
							.connectTimeout(60, TimeUnit.SECONDS)
							.writeTimeout(30, TimeUnit.SECONDS).build();
		}
	}
	/**
	 * @param url
	 */
	public static String get(String url) {
		initClient();
		Request request = new Request.Builder().url(url).method("GET", null).build();
		Call call = asynokHttpClient.newCall(request);
		Response response = null;
		try {
			response = call.execute();
			// String str = response.body().string();
			String str = IOUtils.toString(new BufferedInputStream(response.body().byteStream()));
			return str;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * @param url
	 */
	public static String delete(String url, String paramsJson) {
		initClient();
		RequestBody body = RequestBody.create(MediaType.parse("application/json"), paramsJson);
		Request request = new Request.Builder().url(url).method("DELETE", body).build();
		Call call = asynokHttpClient.newCall(request);
		Response response = null;
		try {
			response = call.execute();
			// String str = response.body().string();
			String str = IOUtils.toString(new BufferedInputStream(response.body().byteStream()));
			return str;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * @param url
	 */
	public static String getWithParameter(String url, Map<String, String> para) throws IOException {
		initClient();
		FormBody.Builder formBody = new FormBody.Builder();
		para.forEach(
				(x, y) -> {
					formBody.add(x, y);
				});
		Request request = new Request.Builder().url(url).method("GET", formBody.build()).build();
		Call call = asynokHttpClient.newCall(request);
		Response response = call.execute();
		String str = IOUtils.toString(new BufferedInputStream(response.body().byteStream()));
		return str;
	}

	/**
	 * @param url
	 * @param callback
	 */
	public static void asynGet(String url, Callback callback) {
		initClient();
		Request request = new Request.Builder().url(url).method("GET", null).build();
		Call call = asynokHttpClient.newCall(request);
		call.enqueue(callback);
		call.cancel();
	}

	public static void closeAsyn() {
	}

	public static String post(String url, String json) throws IOException {
		initClient();
//		RequestBody formBody =
//				new FormBody.Builder().add("username", "test").add("password", "test").build();
		RequestBody body = RequestBody.create(MediaType.parse("application/json"), json);

		Request request = new Request.Builder().url(url).post(body).build();

		Call call = asynokHttpClient.newCall(request);
		Response response = call.execute();
		String str = IOUtils.toString(new BufferedInputStream(response.body().byteStream()));
		return str;
	}

	public static String postJson(String url, String json) {
		initClient();
		RequestBody body = RequestBody.create(MediaType.parse("application/json"), json);
		Request request = new Request.Builder().url(url).post(body).build();

		Call call = asynokHttpClient.newCall(request);
		Response response = null;
		try {
			response = call.execute();
			String str = IOUtils.toString(new BufferedInputStream(response.body().byteStream()));
			return str;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public static String postJsonWithParam(String url, String json, Map<String, String> para) {
		initClient();
		FormBody.Builder formBody =
				new FormBody.Builder();
		para.forEach(
				(x, y) -> {
					formBody.add(x, y);
				});

		Request request = new Request.Builder().url(url).post(formBody.build()).build();

		Call call = asynokHttpClient.newCall(request);
		Response response = null;
		try {
			response = call.execute();
			String str = IOUtils.toString(new BufferedInputStream(response.body().byteStream()));
			return str;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public static void main(String[] args) throws IOException {
//		String json = "{\"dbName\":\"hive.test\",\"tblName\":\"test2\"}";
//		delete("http://localhost:80/api/v1/ddl/table/drop", json);

		String json = "{\"alterTableSql\":\"alter table hive.test.src_kafka_msg set ('properties.group.id'='lmq')\"}";
		System.out.println(post("http://localhost:80/api/v1/ddl/table/alter", json));

//		String json = "{\"addColumnsSQL\":\"alter table hive.test.test add columns(hello_world string)\"}";
//		System.out.println(post("http://localhost:80/api/v1/ddl/table/addcolumns", json));
	}
}
