package com.pactera.yhl.flinkdemo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.Map;

public class JsonDeserialization implements DeserializationSchema<Row> {
    private Map<String,String> fields;
    private RowTypeInfo rowTypeInfo;
    private TypeInformation<?>[] typeInformations;
    private String[] fieldNames;
    public JsonDeserialization(String[] fieldNames,TypeInformation<?>[] typeInformations){
        this.fieldNames = fieldNames;
        this.typeInformations = typeInformations;
        this.rowTypeInfo = new RowTypeInfo(typeInformations,fieldNames);
    }

    @Override
    public Row deserialize(byte[] bytes) throws IOException {
        String message = new String(bytes);
        Row row = new Row(fieldNames.length);
        JSONObject jsonObject = JSON.parseObject(message);
        for (int i = 0; i < fieldNames.length; i++) {
            if(typeInformations[i].getTypeClass() == String.class){
                row.setField(0,jsonObject.getString(fieldNames[i]));
            }else if(typeInformations[i].getTypeClass() == Integer.class){
                row.setField(1,jsonObject.getInteger(fieldNames[i]));
            }
            
        }



        return row;
    }



    @Override
    public boolean isEndOfStream(Row row) {
        return false;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return null;
    }
}
