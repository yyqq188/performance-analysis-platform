package com.pactera.yhl.apps.measure.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AssessmentRateConfig {
    public String key_id;
    public String date_id;
    public String product_code;
    public String product_name;
    public String pay_period;
    public String symbol;
    public String rate;
    public String start_date;
    public String end_date;
    public String state;
    public String load_date;
}
