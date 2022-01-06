package com.sql.gateway.rest;

import com.alibaba.fastjson.JSON;
import com.http.util.OkHttp3Client;

import java.io.IOException;

public class FlinkSqlGatewayEntry {
    public static void main(String[] args) throws Exception {
        String sessionId = "4ba136bfa136f56872c5db2220eb548b";
        String jobId = "50bebf921debf4f4a8b46464a336f6ea";
        String result = getResult(sessionId,jobId, 2);
        System.out.println(result);
//        create(sessionId, "kafka_json");
//        select(sessionId, "kafka_json");
    }


    public static String createSession() throws IOException {
        String json = "{\n" +
                "\t\"planner\": \"blink\",\n" +
                "    \"execution_type\": \"streaming\"" +
                "}";
        String sessionId = JSON.parseObject(OkHttp3Client.postJson("http://localhost:8083/v1/sessions", json)).getString("session_id");
        System.out.println(sessionId);
        return sessionId;
    }

    public static void create(String sessionId, String tableName) throws IOException {
        String sql = "CREATE TABLE " + tableName + " (" +
                "  `msg` VARCHAR" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'test'," +
                "  'properties.bootstrap.servers' = 'localhost:9092'," +
                "  'properties.group.id' = 'test'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'format' = 'json'" +
                ")";

        String createSQL = "{ \"statement\": \"" + sql + "\"}";
        System.out.println(OkHttp3Client.postJson("http://localhost:8083/v1/sessions/" + sessionId + "/statements", createSQL));

    }

    public static void select(String sessionId, String tableName) throws IOException {
        String querySQl = "select msg,count(1) from  " + tableName + " group by msg";
        String queryReq = "{ \"statement\": \"" + querySQl + "\"}";
        System.out.println(OkHttp3Client.postJson("http://localhost:8083/v1/sessions/" + sessionId + "/statements", queryReq));
    }

    public static void insert() {

    }

    public static String getResult(String sessionId, String jobid, int token) throws IOException {
        String reqURL = "http://localhost:8083/v1/sessions/" + sessionId + "/jobs/" + jobid + "/result/" + token;
        String result = OkHttp3Client.get(reqURL);
//        System.out.println(result);
        return result;
    }



}
