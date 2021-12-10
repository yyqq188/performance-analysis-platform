package com.pactera.yhl.joinstr;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

public class JoinStr {
    public static void joinstr(){
        String sql = "";
        SqlParser.Config config = SqlParser.configBuilder().setLex(Lex.MYSQL).build();
        SqlParser sqlParser = SqlParser.create(sql, config);
        SqlNode sqlNode = null;
        try {
             sqlNode = sqlParser.parseStmt();
        } catch (SqlParseException e) {
            e.printStackTrace();
        }
        if(SqlKind.JOIN.equals(sqlNode.getKind())){
            SqlJoin sqlJoin = (SqlJoin) sqlNode;

        }
        


    }
    
}
