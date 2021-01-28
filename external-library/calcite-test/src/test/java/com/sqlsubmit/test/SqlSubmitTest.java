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
        for (int i = 0; i < calls.size(); i++) {
            System.out.println();
        }
    }
}
