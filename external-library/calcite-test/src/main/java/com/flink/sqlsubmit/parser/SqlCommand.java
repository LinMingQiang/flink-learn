package com.flink.sqlsubmit.parser;

import com.flink.sqlsubmit.parser.SqlCommandParser;

import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;

public enum SqlCommand {

    INSERT_INTO(
            "(INSERT\\s+INTO.*)",
            SqlCommandParser.SINGLE_OPERAND),

    CREATE_TABLE(
            "(CREATE\\s+TABLE.*)",
            SqlCommandParser.SINGLE_OPERAND),
    // 括号分组， group匹配三个组，得到 a=b a b三个结果
    SET(
            "SET(\\s+(\\S+)\\s*=(.*))?", // whitespace is only ignored on the left side of '='
            (operands) -> {
                if (operands.length < 3) {
                    return Optional.empty();
                } else if (operands[0] == null) {
                    return Optional.of(new String[0]);
                }
                return Optional.of(new String[]{operands[1], operands[2]});
            }),
    // 自定义一个创建视图的
    ASSIGNMENT("CREATE\\s+VIEW\\s+(\\S+)\\s+AS\\s+(SELECT.*)",
            (operands) ->

            {
                if (operands.length < 2) {
                    return Optional.empty();
                }
                return Optional.of(new String[]{operands[1], operands[0]});
            });


    public final Pattern pattern;
    public final Function<String[], Optional<String[]>> operandConverter;

    SqlCommand(String matchingRegex, Function<String[], Optional<String[]>> operandConverter) {
        this.pattern = Pattern.compile(matchingRegex, SqlCommandParser.DEFAULT_PATTERN_FLAGS);
        this.operandConverter = operandConverter;
    }

    @Override
    public String toString() {
        return super.toString().replace('_', ' ');
    }

    public boolean hasOperands() {
        return operandConverter != SqlCommandParser.NO_OPERANDS;
    }
}
