package com.pactera.yhl.join;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;

public class Demo1 {
    public static void main(String[] args) throws SqlParseException {
        String sql = "select * from orders o join gdsInfo g on o.gdsId=g.gdsId";
        SqlParser.Config config = SqlParser.configBuilder().setLex(Lex.MYSQL).build();
        SqlParser sqlParser = SqlParser.create(sql, config);
        SqlNode sqlNode = sqlParser.parseStmt();
        String leftTable = "";
        String rightTable = "";
        SqlSelect sqlSelect = null;
        if(SqlKind.SELECT.equals(sqlNode.getKind())){
            sqlSelect = (SqlSelect) sqlNode;
            SqlNode where = sqlSelect.getWhere();
            SqlNode from = sqlSelect.getFrom();
            if(SqlKind.JOIN.equals(from.getKind())){
                SqlJoin sqlJoin = (SqlJoin) from;
                SqlNode left = sqlJoin.getLeft();
                SqlNode right = sqlJoin.getRight();
                leftTable =parserTableName(left);
                rightTable = parserTableName(right);
                System.out.println("left "+parserTableName(left));
                System.out.println("right "+parserTableName(right));
            }
        }

        String newTable = leftTable+"_"+rightTable;
        SqlParserPos pos = new SqlParserPos(0,0);
        SqlIdentifier sqlIdentifier = new SqlIdentifier(newTable, pos);
        sqlSelect.setFrom(sqlIdentifier);
        System.out.println(sqlSelect);



    }
    public static String parserTableName(SqlNode tbl){
        if(SqlKind.AS.equals(tbl.getKind())){
            SqlBasicCall sqlBasicCall = (SqlBasicCall) tbl;
//            for(SqlNode sqlNode1 : sqlBasicCall.operands){
//                System.out.println(sqlNode1);
//            }
            return sqlBasicCall.operands[1].toString();
        }
        return "";
    }
}
