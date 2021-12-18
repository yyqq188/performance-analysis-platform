package com.pactera.yhl.apps.construction.entity;

import lombok.Data;

import java.io.Serializable;

/**
 * @author: TSY
 * @create: 2021/11/30 0030 下午 20:59
 * @description:
 */
@Data
public class PremiumsPOJO implements Serializable {
    //算入职
    public String key_id;//主键
    public String rank;//职级
    public String branch_name;//公司名称
    public String stat;//是否签约在职。1为签约在职，2为离职

    //算人力
    public String sales_id;//人员ID
    public String prem;//保费
    public String probation_date;//签约日期
    public String signdate;//签单日期
    public String mark;//标志
}
