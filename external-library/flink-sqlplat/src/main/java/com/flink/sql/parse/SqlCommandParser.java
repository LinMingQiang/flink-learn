package com.flink.sql.parse;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Simple parser for determining the type of command and its parameters.
 */
public final class SqlCommandParser {

    private SqlCommandParser() {
    }

    /**
     * @return
     */
    public static List<SqlCommandCall> parse(String file) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(file));
        List<SqlCommandCall> calls = new ArrayList<>();
        StringBuilder stmt = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
            // 跳过注释
            if (line.trim().isEmpty() || line.startsWith("--")) {
                // skip empty line and comment line
                continue;
            }
            // 把注释内容去掉；以 ； 为一个sql结束
            stmt.append("\n").append(line.replaceAll("--.*", ""));
            if (line.trim().endsWith(";")) {
                Optional<SqlCommandCall> optionalCall = parseSQL(stmt.toString());
                if (optionalCall.isPresent()) {
                    calls.add(optionalCall.get());
                } else {
                    throw new RuntimeException("Unsupported command '" + stmt.toString() + "'");
                }
                // clear string builder
                stmt.setLength(0);
            }
        }
        br.close();
        return calls;
    }

    /**
     * 匹配sql语句，看看是哪个类型，是insert，create等
     *
     * @param stmt
     * @return
     */
    public static Optional<SqlCommandCall> parseSQL(String stmt) {
        // normalize
        stmt = stmt.trim();
        // remove ';' at the end
        if (stmt.endsWith(";")) {
            stmt = stmt.substring(0, stmt.length() - 1).trim();
        }
        // parse
        for (SqlCommand cmd : SqlCommand.values()) {
            final Matcher matcher = cmd.pattern.matcher(stmt);
            if (matcher.matches()) {
                return Optional.of(new SqlCommandCall(SqlCommand.INSERT_INTO, stmt));
            }
        }
        return Optional.of(new SqlCommandCall(SqlCommand.DEFAULT, stmt));
    }

    // --------------------------------------------------------------------------------------------

    private static final Function<String, Optional<String>> NO_OPERANDS =
            (operands) -> Optional.of("");

    private static final Function<String, Optional<String>> SINGLE_OPERAND =
            (operands) -> Optional.of(operands);

    private static final int DEFAULT_PATTERN_FLAGS = Pattern.CASE_INSENSITIVE | Pattern.DOTALL;

    /**
     * Supported SQL commands.
     */
    public enum SqlCommand {
        INSERT_INTO(
                "(INSERT\\s+INTO.*)",
                SINGLE_OPERAND),

        // CREATE [TEMPORARY] VIEW [IF NOT EXISTS] [catalog_name.][db_name.]view_name
        //  [{columnName [, columnName ]* }] [COMMENT view_comment]
        //  AS query_expression
        // flink sql 支持 这个，不需要单独例外处理
//        CREATE_TABLE(
//                "(CREATE\\s+TABLE.*)",
//                SINGLE_OPERAND),
//        SET(
//                "SET(\\s+(\\S+)\\s*=(.*))?", // whitespace is only ignored on the left side of '='
//                (operands) -> {
//                    if (operands.length < 3) {
//                        return Optional.empty();
//                    } else if (operands[0] == null) {
//                        return Optional.of(new String[0]);
//                    }
//                    return Optional.of(new String[]{operands[1], operands[2]});
//                }),

        DEFAULT("DEFAULT", SINGLE_OPERAND),
        // CREATE [TEMPORARY] VIEW [IF NOT EXISTS] [catalog_name.][db_name.]view_name
        //  [{columnName [, columnName ]* }] [COMMENT view_comment]
        //  AS query_expression
        // flink 支持这个语句建表
//        ASSIGNMENT(
//                "CREATE\\s+VIEW\\s+(\\S+)\\s+AS\\s+(SELECT.*)", // whitespace is only ignored on the left side of '='
//                        (operands) -> {
//            if (operands.length < 2) {
//                return Optional.empty();
//            }
//            return Optional.of(new String[]{operands[1], operands[0]});
//        })
        ;

        public final Pattern pattern;
        public final Function<String, Optional<String>> operandConverter;

        SqlCommand(String matchingRegex, Function<String, Optional<String>> operandConverter) {
            this.pattern = Pattern.compile(matchingRegex, DEFAULT_PATTERN_FLAGS);
            this.operandConverter = operandConverter;
        }

        @Override
        public String toString() {
            return super.toString().replace('_', ' ');
        }

        public boolean hasOperands() {
            return operandConverter != NO_OPERANDS;
        }
    }

    /**
     * Call of SQL command with operands and command type.
     */
    public static class SqlCommandCall {
        public final SqlCommand command;
        public final String sql;

        public SqlCommandCall(SqlCommand command, String sql) {
            this.command = command;
            this.sql = sql;
        }

        public SqlCommandCall(SqlCommand command) {
            this(command, "");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SqlCommandCall that = (SqlCommandCall) o;
            return command == that.command && sql == that.sql;
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(command);
            result = 31 * result + sql.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return command + " : (" + sql + ")";
        }
    }
}

