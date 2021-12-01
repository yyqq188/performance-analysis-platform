package com.pactera.yhl.join;

import lombok.Data;

import java.util.Map;
import java.util.Properties;

@Data
public class TableInfo {
    public String tableName;
    public Map<String,String> fieldInfo;
    public Properties props;
    public Boolean isSideTable;
}
