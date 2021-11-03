package com.pactera.yhl.sink;

import com.pactera.yhl.sink.abstr.AbstractHbaseSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class PremiumsHbaseSink extends AbstractHbaseSink<Tuple2<String,Long>> {
    public PremiumsHbaseSink(){
        this.tableName = "testyhl";
    }
    @Override
    public void handle(Tuple2<String,Long> value, Context context, HTable table) throws Exception {
        System.out.println(value);
        Put put = new Put(Bytes.toBytes(value.f0));
        put.addColumn(cf,Bytes.toBytes("ff"),Bytes.toBytes(value.f1.toString()));
        table.put(put);


    }
}
