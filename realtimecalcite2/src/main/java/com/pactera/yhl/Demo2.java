package com.pactera.yhl;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

public class Demo2 {
    public static void main(String[] args) throws SqlParseException {
        SqlParser.Config config = SqlParser.configBuilder().setLex(Lex.MYSQL).build();
        SqlParser sqlParser = SqlParser.create("select id ,name ,age from student where age < 10", config);
        SqlNode sqlNode = sqlParser.parseStmt();
        //得到SqlSelect
        if(SqlKind.SELECT.equals(sqlNode.getKind())){
            SqlSelect sqlSelect =  (SqlSelect) sqlNode;
            SqlNode from = sqlSelect.getFrom();
            SqlNode where = sqlSelect.getWhere();
            SqlNodeList selectList = sqlSelect.getSelectList();
            System.out.println(from);
            System.out.println(where);
            System.out.println(selectList);


            if(SqlKind.IDENTIFIER.equals(from.getKind())){
                System.out.println(from.toString());
            }
            System.out.println("======");
            if(SqlKind.LESS_THAN.equals(where.getKind())){
                for(SqlNode sqlNode1:((SqlBasicCall) where).operands){
                    if(SqlKind.LITERAL.equals(sqlNode1.getKind())){
                        System.out.println(sqlNode1.toString());
                    }
                    if(SqlKind.IDENTIFIER.equals(sqlNode1.getKind())){
                        System.out.println(sqlNode1.toString());
                    }
                }
            }

            System.out.println("======");
            selectList.getList().forEach(x->{
                if(SqlKind.IDENTIFIER.equals(x.getKind())){
                    System.out.println(x.toString());
                }
            });
        }else if(SqlKind.CREATE_TABLE.equals(sqlNode.getKind())){
            System.out.println("create table");
        }



    }
}
