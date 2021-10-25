package com.pactera.yhl.flinkck.map;

import com.pactera.yhl.flinkck.entity.FPolicy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.hadoop.hbase.client.HTable;

public class FPolicyMap extends AbstractMap<String,FPolicy>{

    @Override
    public FPolicy handle(String rowkey, HTable hTable) throws Exception {
        return null;
    }
}
