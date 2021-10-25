package com.pactera.yhl.f_policy.sink;

import com.alibaba.fastjson.JSONObject;

import com.pactera.yhl.entity.Ljtempfeeclass;
import com.pactera.yhl.util.Util;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class TmpRowNumLjtempfeeclassSink extends AbstractInsertHbaseAndKafka<Ljtempfeeclass> {
    public TmpRowNumLjtempfeeclassSink(){
        tableName = "kl:RowNumLjtempfeeclass";
        rowkeys = new String[]{"confmakedate","otherno"};
        columnNames = new String[]{};
        columnTableName = "ljtempfeeclass";
        topic = "topic11";
    }
    @Override
    public void handle(Ljtempfeeclass value, Context context, HTable hTable) throws Exception {
        Get get = new Get(Bytes.toBytes(String.join("",rowkeys)));
        get.addColumn(cf,Bytes.toBytes(columnTableName));
        Result result = hTable.get(get);
        for(Cell cell:result.listCells()){
            byte[] bytes = CellUtil.cloneValue(cell);
            if(bytes.length != 0){
                //比较
                Ljtempfeeclass ljtempfeeclassHbase = JSONObject.parseObject(new String(bytes, "utf-8"), Ljtempfeeclass.class);
                if(ljtempfeeclassHbase.getConfmakedate().compareTo(value.getConfmakedate())>0
                || ljtempfeeclassHbase.getOtherno().compareTo(value.getOtherno())>0){
                    //满足条件后 插入hbase,并写入kafka
                    byte[] columnName = CellUtil.cloneQualifier(cell);

                    Util.insertHbaseAndKafka(String.join("",rowkeys),
                            value,
                            new String(columnName,"utf-8"),
                            hTable,
                            producer,topic);
                }
            }else{
                 //直接插入hbase，并写入kafka
                Util.insertHbaseAndKafka(String.join("",rowkeys),
                        value,
                        columnTableName,
                        hTable,
                        producer,topic);
            }
        }

    }
}
