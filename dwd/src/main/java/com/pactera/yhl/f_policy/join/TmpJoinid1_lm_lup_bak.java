package com.pactera.yhl.f_policy.join;


import com.pactera.yhl.entity.Lccont;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class TmpJoinid1_lm_lup_bak extends AbstractJoin<Lccont, Lccont>{
    private AsyncTable<AdvancedScanResultConsumer> Ld1Table;
    private AsyncTable<AdvancedScanResultConsumer> LmTable;
    private AsyncTable<AdvancedScanResultConsumer> lupTable;
    private AsyncTable<AdvancedScanResultConsumer> kuanbiaoTable;

    @Override
    public void genTableConnection(CompletableFuture conn, Configuration hbaseConfig) throws Exception{
        if(null == conn){
            conn = ConnectionFactory.createAsyncConnection(hbaseConfig);
            connection = (AsyncConnection) conn.get(2, TimeUnit.SECONDS);
            Ld1Table = connection.getTable(TableName.valueOf("taikang:MIDTABLE23"));
            LmTable = connection.getTable(TableName.valueOf("taikang:KUANBIAO_AGENT"));
            lupTable = connection.getTable(TableName.valueOf("taikang:KUANBIAO_AGENT"));
        }

    }

    @Override
    public void asyncHandler(Lccont lccont) throws Exception {
//        String b = lccont.b();
//        Result result = getHbaseResult(b+"",LmTable);
//        for(Cell cell:result.listCells()){
//            String qualifierName = Bytes.toString(CellUtil.cloneQualifier(cell));
//            Result hbaseResult2 = getHbaseResult(a, lupTable);
//            for(Cell cell2:hbaseResult2.listCells()){
//                String qualifierName = Bytes.toString(CellUtil.cloneQualifier(cell));
//                String value = Bytes.toString(CellUtil.cloneValue(cell));
//                Object a = JSONObject.parseObject(value).get("aa");
//
//                Result result = getHbaseResult(a+"",table);
//                for(Cell cell2:hbaseResult2.listCells()){
//                    tableResult.put(
//                            new Put(Bytes.toBytes(cownnum)) //"table" + chdrcoy+chdrnum
//                                    .addColumn(cf,Bytes.toBytes(qualifierName +chdrcoy+chdrnum+""),
//                                            Bytes.toBytes(value))).get();
//
//                }
//            }
//        }
    }




}
