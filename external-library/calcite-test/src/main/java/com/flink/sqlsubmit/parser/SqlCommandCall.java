package com.flink.sqlsubmit.parser;

import java.util.Arrays;
import java.util.Objects;

public class SqlCommandCall {
    public final SqlCommand command;
    public final String[] operands;

    /**
     * 匹配对应的命令，command 包含三种 CREATE,INSERT,SET
     * @param command
     * @param operands
     */
    public SqlCommandCall(SqlCommand command, String[] operands) {
        this.command = command;
        this.operands = operands;
    }

    public SqlCommandCall(SqlCommand command) {
        this(command, new String[0]);
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
        return command == that.command && Arrays.equals(operands, that.operands);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(command);
        result = 31 * result + Arrays.hashCode(operands);
        return result;
    }

    @Override
    public String toString() {
        return command + "(" + Arrays.toString(operands) + ")";
    }
}
