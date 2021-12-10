package com.pactera.yhl.apps.develop.premiums.entity;

import com.pactera.yhl.entity.source.KLEntity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProductRateConfig implements Serializable, KLEntity {
    public String key_id;
    public String date_id;
    public String product_code;
    public String product_name;
    public String pay_period;
    public String rate;
    public String start_date;
    public String end_date;
    public String period_type;
    public String state;
    public String load_date;
}
