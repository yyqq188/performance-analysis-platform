package com.pactera.yhl.f_policy.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.entity.KLEntity;

import com.pactera.yhl.entity.T01teaminfo;
import com.pactera.yhl.util.Util;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zy
 * @version 1.0
 * @description:
 * @date 2021/10/15 9:19
 */
public class T01teaminfo_Team_idSink  extends AbstractInsertHbase<KLEntity>{

    public T01teaminfo_Team_idSink(){
        tableName = "KL:T01TEAMINFO_TEAM_ID";//HBase中间表名
        rowkeys = new String[]{"team_id"};//HBase rowkeys
        //POJO主键---HBase列名
        columnNames = new String[]{"team_id", "channel_id"};
        columnTableName = "t01teaminfo";//表名
    }
    @Override
    public void handle(KLEntity klEntity, Context context, HTable table) throws Exception {
        T01teaminfo value = (T01teaminfo)  klEntity;
        StringBuilder rowkeySb = new StringBuilder();
        StringBuilder columnSb = new StringBuilder();
        columnSb.append(columnTableName);
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
