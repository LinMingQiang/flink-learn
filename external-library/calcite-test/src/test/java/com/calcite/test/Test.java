package com.calcite.test;

import com.calcite.sql.parser.impl.CalciteTestSqlParserImpl;
import com.flink.calcite.sql.sqlnode.CustomSqlSelectEmit;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.sql.parser.dql.SqlShowCatalogs;

import java.util.Arrays;

public class Test {

    public static void main(String[] args) {
        StringBuffer sb = new StringBuffer();
        String tmp = "";
        String[] ss = "helloworld234".split("");
        int mid = ss.length/2;
        int i = 0;
        int j = ss.length-1;
        while(i<mid){
            tmp = ss[i];
            ss[i] = ss[j];
            ss[i]=tmp;
            i++;
            j--;
        }
        System.out.println(ss);
    }
}
