package com.pactera.yhl.sink;

import com.pactera.yhl.entity.source.Lcpol;
import com.pactera.yhl.sink.abstr.AbstractKuduSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Upsert;

public class PremiumsKuduSink extends AbstractKuduSink<Tuple2<String,Long>> {
    public PremiumsKuduSink(){
        tableName = "table1";
    }
    @Override
    public void handler(Tuple2<String,Long> value, Context context) throws Exception {
        Upsert upsert = table.newUpsert();
        PartialRow row = upsert.getRow();
        row.addString(0, value.f0);
        row.addString(1, value.f1.toString());
        session.apply(upsert);
        session.flush(); //手动刷新提交
        session.close(); //要加上close
    }
}
