package com.flink.calcite.sql.sqlnode;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

import javax.annotation.Nonnull;
import java.util.List;

public class CustomSqlSubmit extends SqlCall {
    public static final SqlSpecialOperator OPERATOR;

    SqlNode jobString;

    public CustomSqlSubmit(SqlParserPos pos, SqlNode jobString) {
        super(pos);

        this.jobString = jobString;
    }

    public SqlNode getJobString() {
        return jobString;
    }


    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {

        writer.keyword("SUBMIT JOB AS");
        this.jobString.unparse(writer, leftPrec, rightPrec);
    }
    static {
        OPERATOR = new SqlSpecialOperator("SUBMIT JOB", SqlKind.OTHER_DDL);
    }
}

