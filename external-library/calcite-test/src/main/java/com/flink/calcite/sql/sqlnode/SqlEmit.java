package com.flink.calcite.sql.sqlnode;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.SqlIntervalLiteral.IntervalValue;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

import javax.annotation.Nonnull;

public class SqlEmit extends SqlCall {
    public static final SqlSpecialOperator OPERATOR;
    static final SqlPostfixOperator BEFORE_COMPLETE_OPERATOR;
    static final SqlPostfixOperator AFTER_COMPLETE_OPERATOR;
    SqlNode beforeDelay;
    SqlNode afterDelay;

    public SqlEmit(SqlParserPos pos, SqlNode beforeDelay, SqlNode afterDelay) {
        super(pos);
        this.beforeDelay = beforeDelay;
        this.afterDelay = afterDelay;
    }

    public long getBeforeDelayValue() {
        return this.getDelayValue(this.beforeDelay);
    }

    public long getAfterDelayValue() {
        return this.getDelayValue(this.afterDelay);
    }

    public SqlNode getBeforeDelay() {
        return this.beforeDelay;
    }

    public SqlNode getAfterDelay() {
        return this.afterDelay;
    }

    private long getDelayValue(SqlNode delay) {
        if (delay == null) {
            return -9223372036854775808L;
        } else {
            return isWithoutDelay(delay) ? 0L : ((BigDecimal)((SqlLiteral)delay).getValueAs(BigDecimal.class)).longValue();
        }
    }

    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }


    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.newlineAndIndent();
        writer.keyword("EMIT");
        writer.newlineAndIndent();
        if (this.beforeDelay != null) {
            this.unparse(this.beforeDelay, writer);
            writer.keyword("BEFORE WATERMARK");
        }

        if (this.afterDelay != null) {
            if (this.beforeDelay != null) {
                writer.print(",");
                writer.newlineAndIndent();
            }

            this.unparse(this.afterDelay, writer);
            writer.keyword("AFTER WATERMARK");
        }

    }

    private void unparse(SqlNode delay, SqlWriter writer) {
        if (isWithoutDelay(delay)) {
            delay.unparse(writer, 0, 0);
        } else {
            writer.keyword("WITH DELAY");
            SqlIntervalLiteral interval = (SqlIntervalLiteral)delay;
            IntervalValue value = (IntervalValue)interval.getValue();
            writer.literal("'" + value.getIntervalLiteral() + "'");
            writer.keyword(value.getIntervalQualifier().toString());
        }

    }

    public static SqlEmit create(SqlParserPos pos, List<SqlNode> strategies) {
        SqlNode beforeDelay = null;
        SqlNode afterDelay = null;
        Iterator var4 = strategies.iterator();

        while(true) {
            while(var4.hasNext()) {
                SqlNode s = (SqlNode)var4.next();
                if (SqlKind.PRECEDING != s.getKind() || beforeDelay != null) {
                    if (SqlKind.FOLLOWING != s.getKind() || afterDelay != null) {
                        throw new RuntimeException("Sql Emit statement shouldn't contain duplicate strategies");
                    }

                    afterDelay = (SqlNode)((SqlCall)s).getOperandList().get(0);
                } else {
                    beforeDelay = (SqlNode)((SqlCall)s).getOperandList().get(0);
                }
            }

            return new SqlEmit(pos, beforeDelay, afterDelay);
        }
    }

    public static SqlNode createWithoutDelay(SqlParserPos pos) {
        return SqlEmit.Delay.WITHOUT_DELAY.symbol(pos);
    }

    public static SqlNode createAfterStrategy(SqlNode delay, SqlParserPos pos) {
        return AFTER_COMPLETE_OPERATOR.createCall(pos, new SqlNode[]{delay});
    }

    public static SqlNode createBeforeStrategy(SqlNode delay, SqlParserPos pos) {
        return BEFORE_COMPLETE_OPERATOR.createCall(pos, new SqlNode[]{delay});
    }

    public static boolean isWithoutDelay(SqlNode node) {
        if (node instanceof SqlLiteral) {
            SqlLiteral literal = (SqlLiteral)node;
            if (literal.getTypeName() == SqlTypeName.SYMBOL) {
                return literal.symbolValue(SqlEmit.Delay.class) == SqlEmit.Delay.WITHOUT_DELAY;
            }
        }

        return false;
    }

    static {
        OPERATOR = new SqlSpecialOperator("EMIT", SqlKind.OTHER_DDL);
        BEFORE_COMPLETE_OPERATOR = new SqlPostfixOperator("BEFORE COMPLETE", SqlKind.PRECEDING, 20, ReturnTypes.ARG0, (SqlOperandTypeInference)null, (SqlOperandTypeChecker)null);
        AFTER_COMPLETE_OPERATOR = new SqlPostfixOperator("AFTER COMPLETE", SqlKind.FOLLOWING, 20, ReturnTypes.ARG0, (SqlOperandTypeInference)null, (SqlOperandTypeChecker)null);
    }

    static enum Delay {
        WITHOUT_DELAY("WITHOUT DELAY");

        private final String sql;

        private Delay(String sql) {
            this.sql = sql;
        }

        public String toString() {
            return this.sql;
        }

        public SqlNode symbol(SqlParserPos pos) {
            return SqlLiteral.createSymbol(this, pos);
        }
    }
}

