package com.pactera.yhl;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

public class Demo2 {
    public static void main(String[] args) throws SqlParseException {

        SqlParser.Config config = SqlParser.configBuilder().setLex(Lex.MYSQL).build();
        SqlParser sqlParser = SqlParser.create("select id ,name ,age from student where age < 10", config);
        SqlNode sqlNode = sqlParser.parseStmt();
        //得到SqlSelect
        if(SqlKind.SELECT.equals(sqlNode.getKind())){
            System.out.println("select ");
        }else if(SqlKind.CREATE_TABLE.equals(sqlNode.getKind())){
            System.out.println("create table");
        }
    }
}
