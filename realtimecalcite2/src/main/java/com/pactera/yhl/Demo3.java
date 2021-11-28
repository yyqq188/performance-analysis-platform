package com.pactera.yhl;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

public class Demo3 {
    public static void main(String[] args) throws SqlParseException {
        String sql = "select cast(amount as CHAR) FROM orders";
        SqlParser.Config config = SqlParser.configBuilder().setLex(Lex.MYSQL).build();
        SqlParser sqlParser = SqlParser.create(sql, config);
        SqlNode sqlNode = sqlParser.parseStmt();
        SqlSelect sqlSelect = (SqlSelect) sqlNode;
        sqlSelect.getSelectList().getList().forEach(x->{
            if(SqlKind.CAST.equals(x.getKind())){
                for(SqlNode sqlNode1 : ((SqlBasicCall) x).operands){
                    System.out.println(sqlNode1);
                }
            }
            System.out.println("=====");
            SqlNode operand = ((SqlBasicCall) x).operands[1];
            SqlDataTypeSpec sqlDataTypeSpec =  (SqlDataTypeSpec) operand;
            System.out.println(sqlDataTypeSpec.getTypeName());


        });
    }
}
