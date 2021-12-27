package com.flink.sql.entry;

import com.flink.sql.env.FlinkEvnBuilder;
import com.flink.sql.parse.SqlCommandParser;
import com.flink.sql.util.FlinkLearnPropertiesUtil;
import com.sql.utl.DDLSQLManager;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

public class FlinkSqlPlatEntry {
    /**
     * {"msg":"1"}
     *
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws Exception {
        FlinkLearnPropertiesUtil.init("/Users/eminem/workspace/flink/flink-learn/dist/conf/application.properties", "FlinkSqlPlatEntry");
        FlinkEvnBuilder.initEnv(FlinkLearnPropertiesUtil.param(),
                FlinkLearnPropertiesUtil.CHECKPOINT_PATH(),
                0L,
                Duration.ofHours(1L));
        StreamTableEnvironment tableEnv = FlinkEvnBuilder.tableEnv;
        StatementSet set = tableEnv.createStatementSet();
        List<SqlCommandParser.SqlCommandCall> commandCalls = SqlCommandParser.parse(args[0]);
        commandCalls.forEach(call -> {
            switch (call.command) {
                case INSERT_INTO:
                    String s = call.sql.replaceFirst("(?i)INSERT", "").replaceFirst("(?i)INTO", "");
                    String tagetTbl = s.substring(0, s.indexOf("(") - 1).trim();
                    String selectSql = s.substring(s.indexOf("(") + 1, s.length() - 1);
                    System.out.println(tagetTbl + ":" + selectSql);
                    set.addInsert(tagetTbl, tableEnv.sqlQuery(selectSql));
                    break;
                default:
                    System.out.println(call.sql);
                    tableEnv.executeSql(call.sql);
            }
        });
        set.execute();
    }
}
