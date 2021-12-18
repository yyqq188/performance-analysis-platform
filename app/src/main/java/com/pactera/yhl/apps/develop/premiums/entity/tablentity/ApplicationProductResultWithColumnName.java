package com.pactera.yhl.apps.develop.premiums.entity.tablentity;


import com.pactera.yhl.entity.source.KLEntity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ApplicationProductResultWithColumnName implements Serializable, KLEntity {
    public String key_id;
    public String day_id;
    public String manage_code;
    public String manage_name;
    public String product_code;
    public String product_name;
    public Double prem_day;
    public Double num_day;
    public Double contribution_day;
    public Double prem_month;
    public Double num_month;
    public Double num_average_month;
    public Double ratio_month;
    public Double prem_quarter;
    public Double num_quarter;
    public Double num_average_quarter;
    public Double ratio_quarter;
    public Double prem_year;
    public Double num_year;
    public Double num_average_year;
    public Double ratio_year;
    public String load_date;
    //追加一个字段，是用于指定插入hbase时的列名
    public String columnName;
}
