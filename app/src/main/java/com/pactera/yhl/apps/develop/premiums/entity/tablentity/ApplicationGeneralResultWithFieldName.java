package com.pactera.yhl.apps.develop.premiums.entity.tablentity;

import com.pactera.yhl.entity.source.KLEntity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ApplicationGeneralResultWithFieldName implements Serializable , KLEntity {
    public String key_id;
    public String day_id;
    public String manage_code;
    public String manage_name;
    public Double prem_day;
    public Double prem_new_day;
    public Double hesi_prem_day;
    public Double approve_prem_day;
    public Double approve_num_day;
    public Double regular_prem_day;
    public Double single_prem_day;
    public Double prem_month;
    public Double prem_new_month;
    public Double hesi_prem_month;
    public Double approve_prem_month;
    public Double approve_num_month;
    public Double regular_prem_month;
    public Double single_prem_month;
    public Double achieving_rate_month;
    public Double plan_month;
    public Double prem_quarter;
    public Double prem_new_quarter;
    public Double hesi_prem_quarter;
    public Double approve_prem_quarter;
    public Double approve_num_quarter;
    public Double regular_prem_quarter;
    public Double single_prem_quarter;
    public Double prem_year;
    public Double prem_new_year;
    public Double hesi_prem_year;
    public Double approve_prem_year;
    public Double approve_num_year;
    public Double regular_prem_year;
    public Double single_prem_year;
    public Double achieving_rate_year;
    public Double plan_year;
    public Double approve_prem_year_last;
    public String source_id;
    public String load_date;
    //追加一个字段，是用于指定插入clickhouse的时候要给到的字段
    public String fieldName;
}
