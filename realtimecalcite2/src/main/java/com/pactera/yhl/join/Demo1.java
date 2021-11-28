package com.pactera.yhl.join;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

public class Demo1 {
    public static void main(String[] args) throws SqlParseException {
        String sql = "select * from orders o join gdsInfo g on o.gdsId=g.gdsId";
        SqlParser.Config config = SqlParser.configBuilder().setLex(Lex.MYSQL).build();
        SqlParser sqlParser = SqlParser.create(sql, config);
        SqlNode sqlNode = sqlParser.parseStmt();
        if(SqlKind.SELECT.equals(sqlNode.getKind())){
            SqlSelect sqlSelect = (SqlSelect) sqlNode;
            SqlNode where = sqlSelect.getWhere();
            SqlNode from = sqlSelect.getFrom();
            if(SqlKind.JOIN.equals(from.getKind())){
                
            }
        }
        System.out.println(sqlNode.getKind());



    }
}
