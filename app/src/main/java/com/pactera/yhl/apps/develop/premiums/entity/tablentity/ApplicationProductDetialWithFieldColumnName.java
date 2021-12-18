package com.pactera.yhl.apps.develop.premiums.entity.tablentity;

import com.pactera.yhl.entity.source.KLEntity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ApplicationProductDetialWithFieldColumnName implements Serializable, KLEntity {
    public String key_id;
    public String day_id;
    public String manage_code;
    public String manage_name;
    public String product_code;
    public String product_name;
    public Double prem_day;
    public Double num_day;
    public Double contribution_day;
    public Double single_prem_day;
    public Double three_year_prem_day;
    public Double five_year_prem_day;
    public Double ten_year_prem_day;
    public Double twenty_year_prem_day;
    public Double prem_month;
    public Double num_month;
    public Double num_average_month;
    public Double contribution_month;
    public Double single_prem_month;
    public Double three_year_prem_month;
    public Double five_year_prem_month;
    public Double ten_year_prem_month;
    public Double twenty_year_prem_month;
    public Double prem_quarter;
    public Double num_quarter;
    public Double num_average_quarter;
    public Double contribution_quarter;
    public Double single_prem_quarter;
    public Double three_year_prem_quarter;
    public Double five_year_prem_quarter;
    public Double ten_year_prem_quarter;
    public Double twenty_year_prem_quarter;
    public Double prem_year;
    public Double num_year;
    public Double num_average_year;
    public Double contribution_year;
    public Double single_prem_year;
    public Double three_year_prem_year;
    public Double five_year_prem_year;
    public Double ten_year_prem_year;
    public Double twenty_year_prem_year;
    public String load_date;
    //追加一个字段，是用于指定插入hbase时的列名
    public String columnName;
    public String fieldName;
    public String valueField;
}
