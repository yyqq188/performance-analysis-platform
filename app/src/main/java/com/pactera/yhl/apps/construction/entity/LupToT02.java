package com.pactera.yhl.apps.construction.entity;

import lombok.Data;

import java.io.Serializable;

/**
 * @author: TSY
 * @create: 2021/11/17 0017 上午 10:12
 * @description:
 */
@Data
public class LupToT02 implements Serializable {
    public String branch_id;
    public String sales_id;
    public String prem;
    public String stat;//状态：是否在职
    public String probation_date;//签约日期
    public String signdate;//签单日期
    public String mark;//标志
}
