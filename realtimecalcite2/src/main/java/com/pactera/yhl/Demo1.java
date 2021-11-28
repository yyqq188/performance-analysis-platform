package com.pactera.yhl;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

public class Demo1 {
    public static void main(String[] args) throws SqlParseException {

        String sql = "select SUM(age),COUNT(level),name from idTable";
        SqlParser.Config config = SqlParser.configBuilder().setLex(Lex.MYSQL).build();
        SqlParser sqlParser = SqlParser.create(sql, config);
        SqlNode sqlNode = sqlParser.parseStmt();
        if(SqlKind.SELECT.equals(sqlNode.getKind())){
            SqlSelect sqlSelect = (SqlSelect) sqlNode;

            sqlSelect.getSelectList().getList().forEach(x->{
                if(SqlKind.OTHER_FUNCTION.equals(x.getKind())){
                    SqlBasicCall sqlBasicCall =  (SqlBasicCall) x;
                    for(SqlNode sqlNode1 : sqlBasicCall.operands){
                        System.out.println(sqlNode1);
                    }

                }
                System.out.println("=====");

                if(SqlKind.IDENTIFIER.equals(x.getKind())){


                    System.out.println(x.toString());
                }

            });
        }

    }
}
