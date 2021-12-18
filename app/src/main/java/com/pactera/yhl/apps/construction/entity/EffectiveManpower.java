package com.pactera.yhl.apps.construction.entity;
import lombok.Data;

import java.io.Serializable;

/**
 * @author: TSY
 * @create: 2021/11/12 0012 上午 11:01
 * @description:  有效人力
 */

@Data
public class EffectiveManpower implements Serializable {
    public String key_id;//主键
    public String sales_id;//人员ID
    public String prem;//保费
    //public String stat;//状态：是否在职
    public String probation_date;//签约日期
    public String signdate;//签单日期
    public String mark;//标志
}
