package com.dbutil;

import com.alibaba.fastjson.JSON;
import okhttp3.*;
import org.apache.commons.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class OkHttp3Client2 {

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

//	public static String post(String url, String json) throws IOException {
//		initClient();
////		RequestBody formBody =
////				new FormBody.Builder().add("username", "test").add("password", "test").build();
//		RequestBody body = RequestBody.create(MediaType.parse("application/json"), json);
//
//		Request request = new Request.Builder().url(url).post(body).build();
//
//		Call call = asynokHttpClient.newCall(request);
//		Response response = call.execute();
//		String str = IOUtils.toString(new BufferedInputStream(response.body().byteStream()));
//		return str;
//	}

	/**
	 * 参数放在 body里面
	 * @param url
	 * @param json
	 * @return
	 */
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

	/**
	 * 参数带在url里面
	 * @param url
	 * @param json
	 * @param para
	 * @return
	 */
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
		String tableName = "hive.temp_vipflink.sqlGateWay_test";
//		String host = "flink-sql-gateway.ai.vip.com:80";
		String host = "10.189.108.75:8080";
// 不能带\n

		String cjson = "{\"createTableSql\":\"CREATE TABLE "+tableName+" (" +
				"    topic VARCHAR METADATA FROM 'topic'," +
				"    `offset` bigint METADATA," +
				"    msg VARCHAR" +
				") WITH (" +
				"    'connector' = 'kafka'," +
				"    'topic' = 'test'," +
				"    'scan.startup.mode' = 'latest-offset'," +
				"    'properties.bootstrap.servers' = 'localhost:9092'," +
				"    'properties.group.id' = 'test'," +
				"    'format' = 'json'," +
				"    'json.ignore-parse-errors' = 'true'" +
				")\"}";
//		postJson("http://"+host+"/api/v1/ddl/table/create", cjson);



//		String json = "{\"dbName\":\"hive.test\",\"tblName\":\"test2\"}";
//		delete("http://localhost:80/api/v1/ddl/table/drop", json);

//		String ajson = "{\"alterTableSql\":\"alter table "+tableName+" set ('properties.group.id'='lmq')\"}";
//		System.out.println(postJson("http://"+host+"/api/v1/ddl/table/alter", ajson));
//
//		String addjson = "{\"addColumnsSQL\":\"alter table "+tableName+" add columns(hello_world string)\"}";
//		System.out.println(postJson("http://"+host+"/api/v1/ddl/table/addcolumns", addjson));

		String sjson = "{\"tableName\":\"temp_vipflink.hudi_nullcol_test\"}";
		JSON data = JSON.parseObject(postJson("http://"+host+"/api/v1/ddl/table/showcreatetable", sjson)) ; //.getString("data");
		System.out.println(data);


//				String json = "{\"tableName\":\"test.kafka_source\"}";
//		JSON data = JSON.parseObject(postJson("http://localhost:80/api/v1/ddl/table/showcreatetable", json)) ; //.getString("data");
//		System.out.println(data);

	}
}
