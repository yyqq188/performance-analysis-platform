package com.pactera.yhl.apps.develop.premiums.premise.mid;

import com.pactera.yhl.entity.source.T02salesinfok;

public class MidSaleinfoK extends AbstractInsertHbase<T02salesinfok> {
    public MidSaleinfoK(){
        tableName = "KLMIDAPP:t02salesinfok_salesId";//HBase中间表名
        rowkeys = new String[]{"sales_id"};
        columnNames = new String[]{""};
        columnTableName = "t02salesinfok";
    }
}

