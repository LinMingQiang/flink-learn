package com.sql.gateway.rest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.http.util.OkHttp3Client;
import org.junit.Test;

import java.io.IOException;

public class FlinkSqlGatewayRESTApiTest {

    /**
     * 组装一个请求的json
     *
     * @param sql
     * @return
     */
    public static String getRequestJsonStr(String sql) {
        return "{ \"statement\": \"" + sql + "\"}";
    }

    /**
     * 创建表
     *
     * @param sessionId
     * @param tableName
     */
    public static void createTable(String sessionId, String tableName, String topic) {
        String createSQL = "CREATE TABLE " + tableName + " (" +
                "  `msg` VARCHAR," +
                "  `money` BIGINT " +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = '"+topic+"'," +
                "  'properties.bootstrap.servers' = 'localhost:9092'," +
                "  'properties.group.id' = 'test'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'format' = 'json'," +
                "  'json.ignore-parse-errors' = 'true'" +
                ")";
        sendReq(sessionId, createSQL);
    }

    /**
     * 创建sessionid
     *
     * @return
     */
    public static String createSession() {
        String json = "{\n" +
                "\t\"planner\": \"blink\",\n" +
                "    \"execution_type\": \"streaming\"" +
                "}";
        String sessionId = JSON.parseObject(OkHttp3Client.postJson("http://localhost:8083/v1/sessions", json)).getString("session_id");
        System.out.println(sessionId);
        return sessionId;
    }

    /**
     * 发送请求并答应结果
     *
     * @param sessionId
     * @param sql
     */
    public static String sendReq(String sessionId, String sql) {
        String res = OkHttp3Client.postJson("http://localhost:8083/v1/sessions/" + sessionId + "/statements", getRequestJsonStr(sql));
        System.out.println(res);
        return res;
    }

    /**
     * 获取流计算的结果并展示
     *
     * @param sessionId
     * @param jobId
     * @param token
     */
    public static void printStreamingResult(String sessionId, String jobId, int token) {
        String reqURL = "http://localhost:8083/v1/sessions/" + sessionId + "/jobs/" + jobId + "/result/" + token;
        String result = OkHttp3Client.get(reqURL);
        System.out.println(result);
    }

    /**
     *
     * @param json
     * @return
     */
    public static String getJobId(String json) {
        JSONArray results = JSON.parseObject(json).getJSONArray("results");
        for (int i = 0; i < results.size(); i++) {
            JSONArray dataArr = JSON.parseObject(results.get(0).toString()).getJSONArray("data");
            return dataArr.getJSONArray(0).get(0).toString();
        }
        return null;
    }

    @Test
    public void testGetStreamingResult(){
        // {"msg":"hello","money":100}
        String sessionId = "d780c778ff7efc8144b39b4d98bf2942";
        String jobId = "1b2c87a932ce12b60a1a213845f65e2e";
        int token = 1;
        printStreamingResult(sessionId, jobId, token);
    }
    @Test
    public void testCreateSession() {
        String json = "{\n" +
                "\t\"planner\": \"blink\",\n" +
                "    \"execution_type\": \"streaming\"" +
                "}";
        String sessionId = JSON.parseObject(OkHttp3Client.postJson("http://localhost:8083/v1/sessions", json)).getString("session_id");
        System.out.println(sessionId);
    }

    @Test
    public void testCreateTable() {
        String sessionId = createSession();
        createTable(sessionId, "test", "test");
        String sTablesSql = "show tables";
        sendReq(sessionId, sTablesSql);
    }

    /**
     * 只有SET 的话是返回所有配置参数， set xxx = xxx 是设置参数
     */
    @Test
    public void testSetSql() {
        String sessionId = createSession();
        String querySQl = "SET";
        sendReq(sessionId, querySQl);
    }

    @Test
    public void testShowInfos() {
        String sessionId = createSession();
        String sCatalogsSql = ("show catalogs");
        String sCCatalogsSql = ("show current catalog");
        String sDatabasesSql = ("show databases");
        String scDatabasesSql = ("show current database");
        String sTablesSql = ("show tables");
        String sFunctionsSql = ("show functions");
        String sModulesSql = ("show modules");
        sendReq(sessionId, sCatalogsSql);
        sendReq(sessionId, sCCatalogsSql);
        sendReq(sessionId, sDatabasesSql);
        sendReq(sessionId, scDatabasesSql);
        sendReq(sessionId, sTablesSql);
        sendReq(sessionId, sFunctionsSql);
        sendReq(sessionId, sModulesSql);
    }

    @Test
    public void testSelect() {
        String tableName = "test";
        String sessionId = createSession();
        createTable(sessionId, tableName, tableName);
        String querySQl = "select msg,sum(money) from  " + tableName + " group by msg";
        sendReq(sessionId, querySQl);
    }

    @Test
    public void testExplain() {
        String tableName = "test";
        String sessionId = createSession();
        createTable(sessionId, tableName, tableName);
        String explainSql = getRequestJsonStr("EXPLAIN PLAN FOR select * from " + tableName);
        sendReq(sessionId, explainSql);
    }

    @Test
    public void testInsert()  {
        String tableName = "test";
        String sessionId = createSession();
        createTable(sessionId, tableName, tableName);
        String querySQl = "select msg,sum(money) from  " + tableName + " group by msg";
        String req = sendReq(sessionId, querySQl);
        String jobId = getJobId(req);
        System.out.println(jobId);
        String insertSql = "insert into " + tableName + " VALUES ('hello', 100), ('word', 200), ('word', 200)";
        sendReq(sessionId, insertSql);

        printStreamingResult(sessionId, jobId, 0);
    }

    /**
     * 测试从一个source到另一个sink
     * @throws InterruptedException
     */
    @Test
    public void testKafkaSink() {
        String sourceTbl = "test";
        String sinkTbl = "test2";
        String sessionId = createSession();
        createTable(sessionId, sourceTbl, sourceTbl);
        createTable(sessionId, sinkTbl, sinkTbl);
        sendReq(sessionId, "show tables");

        // 任务1： 执行insert
        String insertSql = "insert into "+sinkTbl+" select msg, money from  " + sourceTbl ;
        sendReq(sessionId, insertSql);

        // 任务2： 查询sinkTbl
        String querySQl = "select msg,sum(money) as totalMoney from " + sinkTbl + " group by msg";
        String req = sendReq(sessionId, querySQl);
        String jobId = getJobId(req);
        System.out.println(jobId);

        // 插入两数据给 sourceTbl
        String insertValueSql = "insert into " + sourceTbl + " VALUES ('hello', 100), ('word', 200), ('word', 200)";
        sendReq(sessionId, insertValueSql);
        // 查询结果
        printStreamingResult(sessionId, jobId, 0);

    }
}
