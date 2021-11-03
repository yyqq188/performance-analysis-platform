package com.pactera.yhl.sink;

import com.pactera.yhl.sink.abstr.AbstractKuduSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Upsert;

public class PremiumsKuduSinkV2 extends AbstractKuduSink<Tuple4<String,String,String,String>> {
    public PremiumsKuduSinkV2(){
        tableName = "lcpoltest";
    }
    @Override
    public void handler(Tuple4<String,String,String,String> value, Context context) throws Exception {
        Upsert upsert = table.newUpsert();
        PartialRow row = upsert.getRow();
        for (int i = 0; i < value.getArity(); i++) {
            row.addString(i, value.getField(i));
        }
        session.apply(upsert);
        session.flush(); //手动刷新提交
        session.close(); //要加上close
    }
}
