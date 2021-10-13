package com.pactera.yhl.insurance_detail.join;


import com.pactera.yhl.entity.Lccont;
import com.pactera.yhl.entity.Lcpol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class TmpJoinlm_id1_lup extends AbstractJoin<Lccont, Lcpol>{
    private AsyncTable<AdvancedScanResultConsumer> table;
    private AsyncTable<AdvancedScanResultConsumer> tableResult;

    @Override
    public void genTableConnection(CompletableFuture conn, Configuration hbaseConfig) throws Exception{
        if(null == conn){
            conn = ConnectionFactory.createAsyncConnection(hbaseConfig);
            connection = (AsyncConnection) conn.get(2, TimeUnit.SECONDS);
            table = connection.getTable(TableName.valueOf("taikang:MIDTABLE23"));
            tableResult = connection.getTable(TableName.valueOf("taikang:KUANBIAO_AGENT"));
        }
    }

    @Override
    public void asyncHandler(Lccont lccont) throws Exception {
        String cownnum = lccont.getCownnum();
        String chdrcoy = lccont.getChdrcoy();
        String chdrnum = lccont.getChdrnum();
        Result result = getHbaseResult(chdrcoy+chdrnum+"",table);

        for(Cell cell:result.listCells()){
            String qualifierName = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            Result hbaseResult2 = getHbaseResult(value, tableResult);
            for(Cell cell2:hbaseResult2.listCells()){
                //要那个key
                Result result = getHbaseResult(chdrcoy+chdrnum+"",table);
            }

        }






    }

    public static Result getHbaseResult(String rowkey,AsyncTable<AdvancedScanResultConsumer> table) throws Exception{
        String chdrcoy = rowkey;
        Get get = new Get(Bytes.toBytes(chdrcoy+""));
        get.addFamily(cf);
        CompletableFuture<Result> resultCompletableFuture = table.get(get);
        Result result = resultCompletableFuture.get();
        return result;

    }


}
