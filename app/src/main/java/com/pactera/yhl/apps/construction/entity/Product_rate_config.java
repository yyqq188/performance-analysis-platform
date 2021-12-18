package com.pactera.yhl.apps.construction.entity;

import com.pactera.yhl.entity.source.KLEntity;
import lombok.Data;

/**
 * @author: TSY
 * @create: 2021/11/26 0026 下午 18:57
 * @description:
 */
@Data
public class Product_rate_config implements KLEntity {
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
