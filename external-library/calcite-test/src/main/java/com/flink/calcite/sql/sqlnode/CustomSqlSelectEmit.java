package com.flink.calcite.sql.sqlnode;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

import javax.annotation.Nonnull;
import java.util.List;

public class CustomSqlSelectEmit extends SqlCall {
    public static final SqlSpecialOperator OPERATOR;
    public static TimeUnit time;
    public static SqlNode query;
    public static SqlNode wtmstring;
    public CustomSqlSelectEmit(SqlParserPos pos, SqlNode query, SqlNode wtmstring, TimeUnit time) {
        super(pos);
        this.query = query;
        this.wtmstring = wtmstring;
        this.time = time;
    }
    static {
        OPERATOR = new SqlSpecialOperator("SUBMIT JOB", SqlKind.OTHER_DDL);
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.OTHER_DDL;
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {

        writer.keyword("select : ");
        this.query.unparse(writer, leftPrec, rightPrec);
        writer.newlineAndIndent();
        writer.keyword("Emit : ");
        this.wtmstring.unparse(writer, leftPrec, rightPrec);
        writer.newlineAndIndent();
        writer.keyword("Time : " + this.time.toString());
    }
    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }
}
