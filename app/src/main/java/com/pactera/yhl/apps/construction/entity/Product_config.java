package com.pactera.yhl.apps.construction.entity;

import com.pactera.yhl.entity.source.KLEntity;
import lombok.Data;
import org.apache.flink.table.planner.expressions.E;

/**
 * @author: TSY
 * @create: 2021/11/26 0026 下午 18:51
 * @description:
 */
@Data
public class Product_config implements KLEntity {
    public String product_code;
    public String product_payintv;
    public String product_name;
}
