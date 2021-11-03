package com.pactera.yhl.apps.develop.premiums.premise.mid;

import com.pactera.yhl.entity.source.Lcpol;

public class MidLcpol extends AbstractInsertHbase<Lcpol> {
    public MidLcpol(){
        tableName = "KLMIDAPP:lcpol_agentcode";//HBase中间表名
        rowkeys = new String[]{"agentcode"};
        columnNames = new String[]{"polno"};
        columnTableName = "lcpol";
    }
}
