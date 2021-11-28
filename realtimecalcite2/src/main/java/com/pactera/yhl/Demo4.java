package com.pactera.yhl;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

public class Demo4 {
    public static void main(String[] args) {

        SqlOperator sqlOperator = new SqlAsOperator();
        SqlParserPos sqlParserPos = new SqlParserPos(1, 1);
        SqlIdentifier order = new SqlIdentifier("order", null, sqlParserPos);
        SqlIdentifier o = new SqlIdentifier("o", null, sqlParserPos);
        SqlNode[] sqlNodes = new SqlNode[2];
        sqlNodes[0] = order;
        sqlNodes[1] = o;

        SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlOperator.createCall(sqlParserPos,sqlNodes);

        System.out.println(sqlBasicCall);


    }
}
