package com.pactera.yhl.flinkck.map;

import com.pactera.yhl.flinkck.entity.FAgent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.hadoop.hbase.client.HTable;

public class FAgentMap extends AbstractMap<String,FAgent>{

    @Override
    public FAgent handle(String rowkey, HTable hTable) throws Exception {
        return null;
    }
}
