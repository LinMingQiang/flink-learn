package com.sqlsubmit.test;

import com.flink.sqlsubmit.parser.SqlCommandCall;
import com.flink.sqlsubmit.parser.SqlCommandParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class SqlSubmitTest {
    public static void main(String[] args) throws IOException {
        List<String> sql = Files.readAllLines(Paths.get("/Users/eminem/workspace/flink/flink-learn/resources/file/ddl/ddl.sql"));
        List<SqlCommandCall> calls = SqlCommandParser.parse(sql);
        calls.forEach(x -> {
            switch (x.command) {
                case SET:
                    System.out.println("SET " + x.operands[0] + " = " + x.operands[1]);
                    break;
                case INSERT_INTO:
                    System.out.println("INSERT " + x.operands[0]);
                    break;
                case CREATE_TABLE:
                    System.out.println("CREATE " + x.operands[0]);
                    break;
                default:
                    System.out.println("error");
            }
        });
    }
}
