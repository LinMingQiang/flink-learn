package com.sql.gateway.rest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.http.util.OkHttp3Client;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlinkSqlGatewayRESTApiTest {

    /**
     * 组装一个请求的json
     *
     * @param sql
     * @return
     */
    public static String getRequestJsonStr(String sql, String jobName) {
        // 不能带 \n
        if(jobName!=null)
        return "{ \"statement\": \"" + sql + "\",\"job_name\":\""+jobName+"\"}";
        return "{ \"statement\": \"" + sql + "\"}";
    }

    /**
     * 创建表
     *
     * @param sessionId
     * @param tableName
     */
    public static void createTable(String sessionId, String tableName, String topic) {
        String createSQL =
                "CREATE TABLE "
                        + tableName
                        + " ("
                        + "  `msg` VARCHAR,"
                        + "  `money` BIGINT "
                        + ") WITH ("
                        + "  'connector' = 'kafka',"
                        + "  'topic' = '"
                        + topic
                        + "',"
                        + "  'properties.bootstrap.servers' = 'localhost:9092',"
                        + "  'properties.group.id' = 'test',"
                        + "  'scan.startup.mode' = 'latest-offset',"
                        + "  'format' = 'json',"
                        + "  'json.ignore-parse-errors' = 'true'"
                        + ")";
        sendReq(sessionId, createSQL, "createTable_"+tableName);
    }

    /**
     * 创建sessionid
     *  注意： 可使用的参数参考 ExecutionEntry
     *  执行部分的参数配置前缀要加上： execution.
     *  tableConfig的配置加上： table. // TODO
     *  服务配置： server
     *  session配置：session
     *  deployment配置： deployment ：这个是部署参数，应该是flink.conf配置里面的
     * @return
     */
    public static String createSession() {
        String json =
                "{\n"
                        + "\"planner\": \"blink\",\n"
                        + "\"execution_type\": \"streaming\",\n"+
                        " \"properties\":" +
                        "{" +
                            "\"execution.parallelism\":3," +
                            "\"execution.max-parallelism\":3," +
                            "\"table.exec.state.ttl\":1000" +
                        "}\n"
                        + "}";
        System.out.println(json);
        String sessionId =
                JSON.parseObject(OkHttp3Client.postJson("http://localhost:8083/v1/sessions", json))
                        .getString("session_id");
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
        String sqlJson =  getRequestJsonStr(sql, null);
        System.out.println(sqlJson);
        String res =
                OkHttp3Client.postJson(
                        "http://localhost:8083/v1/sessions/" + sessionId + "/statements",
                        sqlJson);
        System.out.println(res);
        return res;
    }
    /**
     * 发送请求并答应结果
     *
     * @param sessionId
     * @param sql
     */
    public static String sendReq(String sessionId, String sql, String jobName) {
        String sqlJson =  getRequestJsonStr(sql, jobName);
        System.out.println(sqlJson);
        String res =
                OkHttp3Client.postJson(
                        "http://localhost:8083/v1/sessions/" + sessionId + "/statements",
                        sqlJson);
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
        String reqURL =
                "http://localhost:8083/v1/sessions/"
                        + sessionId
                        + "/jobs/"
                        + jobId
                        + "/result/"
                        + token;
        String result = OkHttp3Client.get(reqURL);
        System.out.println(result);
    }

    /**
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
    public void testGetStreamingResult() {
        // {"msg":"hello","money":100}
        String sessionId = "d780c778ff7efc8144b39b4d98bf2942";
        String jobId = "1b2c87a932ce12b60a1a213845f65e2e";
        int token = 1;
        printStreamingResult(sessionId, jobId, token);
    }

    @Test
    public void testCreateSession() {
        createSession();
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
        String explainSql = getRequestJsonStr("EXPLAIN PLAN FOR select * from " + tableName, null);
        sendReq(sessionId, explainSql);
    }

    @Test
    public void testInsert() {
        String sourceTbl = "test";
        String sinkTbl = "test2";
        String sessionId = createSession();
        createTable(sessionId, sourceTbl, sourceTbl);
        createTable(sessionId, sinkTbl, sinkTbl);
        // 任务1： 执行insert
        String insertSql = "insert into " + sinkTbl + " select msg, money from  " + sourceTbl;
        sendReq(sessionId, insertSql);
    }

    /**
     * 测试从一个source到另一个sink
     *
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
        String insertSql = "insert into " + sinkTbl + " select msg, money from  " + sourceTbl;
        sendReq(sessionId, insertSql);

        // 任务2： 查询sinkTbl
        String querySQl = "select msg,sum(money) as totalMoney from " + sinkTbl + " group by msg";
        String req = sendReq(sessionId, querySQl);
        String jobId = getJobId(req);
        System.out.println(jobId);

        // 插入两数据给 sourceTbl
        String insertValueSql =
                "insert into " + sourceTbl + " VALUES ('hello', 100), ('word', 200), ('word', 200)";
        sendReq(sessionId, insertValueSql);
        // 查询结果
        printStreamingResult(sessionId, jobId, 0);
    }

    @Test
    public void oneJobMultipleSink() {
        String sourceTbl = "test";
        String sinkTbl = "test2";
        String sinkTbl2 = "test3";
        String sessionId = createSession();
        createTable(sessionId, sourceTbl, sourceTbl);
        createTable(sessionId, sinkTbl, "test2");
        createTable(sessionId, sinkTbl2, "test2");

// 这个方式提交是两个 job
//        // 任务1： 执行insert
        String insertSql = "insert into " + sinkTbl + " select msg,money from  " + sourceTbl + "";
//        sendReq(sessionId, insertSql);
//        // 任务2： 执行insert
        String insertSql2 = "insert into " + sinkTbl + " select msg,money*100 as money from  " + sourceTbl + "";
//        sendReq(sessionId, insertSql2);

// 两种提交方式 这种方式不支持, 经过改写，现在可以支持了，但是 换行要带过去
        // 任务3： 执行insert
        String insertSql3 = insertSql+";\\n" + insertSql2;
        sendReq(sessionId, insertSql3);

        // 插入两数据给 sourceTbl
        String insertValueSql =
                "insert into " + sourceTbl + " VALUES ('hello', 100), ('word', 200), ('word', 200)";
        sendReq(sessionId, insertValueSql);

        // 查询结果
//        printStreamingResult(sessionId, jobId, 0);
    }


    /**
     * 同一个session维持的同一个tableEnv，所以即使分多次请求会启动多个job
     */
    @Test
    public void createViewTest(){
        String sourceTbl = "test";
        String viewTbl = "test2";
        String sessionId = createSession();
        createTable(sessionId, sourceTbl, sourceTbl);
        sendReq(sessionId, "show tables");

        String createViewSQL = String.format("CREATE VIEW %s as select * from %s", viewTbl, sourceTbl);
        System.out.println(createViewSQL);
        sendReq(sessionId, createViewSQL);
        sendReq(sessionId, "show tables");

        String querySQl = "select msg,count(money) as totalMoney from " + sourceTbl + " group by msg";
        String viewQuerySQl = "select msg,sum(money) as totalMoney from " + viewTbl + " group by msg";
        sendReq(sessionId, querySQl);
        sendReq(sessionId, viewQuerySQl);

    }

    @Test
    public void testFromFileGetSQL() throws Exception {
        BufferedReader br = new BufferedReader(new FileReader("/Users/eminem/workspace/flink/flink-learn/resources/file/ddl/flink-sql-gateway.sql"));
        StringBuilder stmt = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
            stmt.append(line);
            stmt.append("\\n");// 必须要有这个，妈的
        }
        br.close();
        String sessionId = createSession();
        System.out.println(stmt);
        sendReq(sessionId, stmt.toString());
    }


}
