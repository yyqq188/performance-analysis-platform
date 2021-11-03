package com.pactera.yhl.apps.develop.premiums.premise.mid;

import com.pactera.yhl.entity.source.Lbpol;

public class MidLbpol extends AbstractInsertHbase<Lbpol> {
    public MidLbpol(){
        tableName = "KLMIDAPP:lbpol_agentcode";//HBase中间表名
        rowkeys = new String[]{"agentcode"};
        columnNames = new String[]{"polno"};
        columnTableName = "lbpol";
    }
}
