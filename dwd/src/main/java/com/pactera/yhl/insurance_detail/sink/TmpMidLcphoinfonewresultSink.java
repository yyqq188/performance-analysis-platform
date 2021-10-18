package com.pactera.yhl.insurance_detail.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.entity.KLEntity;
import com.pactera.yhl.entity.Lbcont;
import com.pactera.yhl.entity.Lcphoinfonewresult;
import com.pactera.yhl.util.Util;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

public class TmpMidLcphoinfonewresultSink extends AbstractInsertHbase<Lcphoinfonewresult>  {
    public TmpMidLcphoinfonewresultSink(){
        tableName = "kl:midlcphoinfonewresult";
        rowkeys = new String[]{"contno"};
        columnNames = new String[]{};
        columnTableName = "lcphoinfonewresult";
    }
    @Override
    public void handle(Lcphoinfonewresult value, Context context, HTable table) throws Exception {
        if(!value.state.equals("02")) return;

        StringBuilder rowkeySb = new StringBuilder();
        StringBuilder columnSb = new StringBuilder();
        try{
            Map<String,Object> map=new HashMap<String,Object>();

            for(String rowkey:rowkeys){
                map.put("value",value);
                map.put("rowkey",rowkey);
                String expression = "value.getStateflag(rowkey)";
                Object value1 = Util.convertToCode(expression,map);
                rowkeySb.append(value1);
            }
            if(columnNames.length > 0 ) {
                for(String rowkey:columnNames){
                    map.put("value",value);
                    map.put("rowkey",rowkey);
                    String expression = "value.getStateflag(rowkey)";
                    Object value1 = Util.convertToCode(expression,map);
                    columnSb.append(value1);
                }
            }

//            Put put = new Put(Bytes.toBytes(value.getChdrcoy() + value.getChdrnum()));
            Put put = new Put(Bytes.toBytes(rowkeySb.toString()));
            String valueJson = JSON.toJSONString(value, SerializerFeature.WriteMapNullValue, SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.WriteDateUseDateFormat);
            put.addColumn(cf, Bytes.toBytes(columnSb.toString()), Bytes.toBytes(valueJson));
            table.put(put);

        }catch (Exception e){

        }
        System.out.println("tableName is "+tableName);
    }
}