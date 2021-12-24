package com.pactera.yhl.apps.develop.kafka2clickhouse;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.pactera.yhl.apps.develop.premiums.entity.tablentity.ApplicationGeneralResult;
import com.pactera.yhl.sink.abstr.MyHbaseCli;
import com.pactera.yhl.util.Util;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class AppGeneralResultHbaseMapFunc extends RichMapFunction<String, String> {
    protected String rowkey;
    protected static final String cfString = "f";
    protected static final byte[] cf = Bytes.toBytes(cfString);
    protected  String tableName;
    private Connection connection;
    protected HTable table;

    public AppGeneralResultHbaseMapFunc(String tableName){
        this.tableName = tableName;

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ParameterTool params = (ParameterTool) getRuntimeContext()
                .getExecutionConfig().getGlobalJobParameters();
        String config_path = params.get("config_path");
        connection = MyHbaseCli.hbaseConnection(config_path);
        table = (HTable) connection.getTable(TableName.valueOf(tableName));

    }

    @Override
    public String map(String s) throws Exception {
        ApplicationGeneralResult obj = new ApplicationGeneralResult();
        Set<String> fieldNameSets = new HashSet<>();
        Field[] declaredFields = obj.getClass().getDeclaredFields();
        for(Field f : declaredFields){
            fieldNameSets.add(f.getName());
        }

        //将rowkey的值赋值到对象中
        setRowkeyValues(s,obj);

        //这里是将hbase的列的值赋值到新的对象中去
        JSONObject jsonObject = JSON.parseObject(s);
        String columnName = jsonObject.get("columnName").toString();
        //原hbase的字段，被减的字段
        String[] hbaseFields = columnName.split(";")[0].split(",");
        //需要减去的字段
        String subField = columnName.split(";")[1];
        String rowkeys = columnName.split(";")[2];
        String rowkeyStr = getRowkeyStr(rowkeys,jsonObject);
        Get get = new Get(Bytes.toBytes(rowkeyStr));
        Result result = table.get(get);

        if(Objects.isNull(result.listCells())) return null;

        for(Cell cell:result.listCells()){
            String qualifierName = new String(CellUtil.cloneQualifier(cell));
            if(fieldNameSets.contains(qualifierName)){
                Double newDoubleValue = Double.valueOf(new String(CellUtil.cloneValue(cell)));
                setNewDoubleValue(qualifierName,obj,newDoubleValue);
            }
        }


        //在这里加处理，对一些值算减法  ,不能放入形参中
        ApplicationGeneralResult newObj = subEntity(hbaseFields, subField, obj);


        return JSON.toJSONString(newObj,
                SerializerFeature.WriteMapNullValue,
                SerializerFeature.DisableCircularReferenceDetect,
                SerializerFeature.WriteDateUseDateFormat);
    }

    //对插入到ck的值，再做一次减法的处理后再插入
    private ApplicationGeneralResult subEntity(String[] hbaseFields,String subField,ApplicationGeneralResult obj) throws Exception{
        Field field = obj.getClass().getField(subField);
        String subVal = field.get(obj).toString();
        BigDecimal subvalDecimal = new BigDecimal(subVal);
        for(String f:hbaseFields){
            String hbaseSourceVal = obj.getClass().getField(f).get(obj).toString();
            BigDecimal hbaseSourceValDecimal = new BigDecimal(hbaseSourceVal);
            double subedVal = hbaseSourceValDecimal.subtract(subvalDecimal).doubleValue();
            setNewDoubleValue(f,obj,subedVal);
        }
        if(subField.equals("hesi_prem_day")){
            Field hesi = obj.getClass().getField(subField);
            Double hesiVal = Double.valueOf(hesi.get(obj).toString());
            setNewDoubleValue("hesi_prem_day",obj,hesiVal * -1);
        }
        return obj;
    }



    private void setRowkeyValues(String jsonStr,ApplicationGeneralResult obj) throws Exception {
        JSONObject jsonObject = JSON.parseObject(jsonStr);
        String columnName = jsonObject.get("columnName").toString();
        String rowkeys = columnName.split(";")[2];
        String[] keys = rowkeys.split(",");
        for(String key:keys){
            String value = jsonObject.getString(key);
            setNewStringValue(key,obj,value);
        }
    }



    //根据指定的字段名找到rowkey
    private String getRowkeyStr(String rowkeys,JSONObject jsonObject) {
        StringBuffer sb = new StringBuffer();
        for (String rowkeyfield : rowkeys.split(",")) {
            String s = jsonObject.get(rowkeyfield).toString();
            sb.append(",");
            sb.append(s);
        }
        //rowkey字符串
        //这是直接发送到hbase
        String rowkeyStr = sb.toString().substring(1, sb.toString().length());
        return rowkeyStr;
    }

    private void setNewDoubleValue(String fieleName,ApplicationGeneralResult value,Double newValue) throws Exception {
        String methodName = "set"+ Util.LargerFirstChar(fieleName);
        Method method = value.getClass().getDeclaredMethod(methodName, Double.class);
        method.invoke(value,newValue);
    }

    private void setNewStringValue(String fieleName,ApplicationGeneralResult value,String newValue) throws Exception {
        String methodName = "set"+ Util.LargerFirstChar(fieleName);
        Method method = value.getClass().getDeclaredMethod(methodName, String.class);
        method.invoke(value,newValue);
    }





    //去掉指定key的jsonObject
    private String getJSONStrSub(String jsonStr,String subKey){
        JSONObject newJsonObject = new JSONObject();
        JSONObject jsonObject = JSON.parseObject(jsonStr);
        Set<String> keys = jsonObject.keySet();
        for(String key:keys){
            if(subKey.equals(key)) continue;
            newJsonObject.put(key,jsonObject.getString(key));
        }
        return JSON.toJSONString(newJsonObject,
                SerializerFeature.WriteMapNullValue,
                SerializerFeature.DisableCircularReferenceDetect,
                SerializerFeature.WriteDateUseDateFormat);
    }




}
