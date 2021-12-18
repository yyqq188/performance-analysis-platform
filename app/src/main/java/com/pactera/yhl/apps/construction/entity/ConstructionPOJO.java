package com.pactera.yhl.apps.construction.entity;

import lombok.Data;

import java.io.Serializable;

/**
 * @author: TSY
 * @create: 2021/11/30 0030 下午 16:17
 * @description:
 */
@Data
public class ConstructionPOJO implements Serializable {
    public String key_id;//主键
    public String day_id;//日期(当天的年月日)
    public String manage_code;//机构代码(分公司)
    public String manage_name;//公司名称
    public String director_enter__day;//日入职总监人数
    public String manager_enter__day;//日入职部经理人数
    public String member_enter__day;//日入职专员人数
    public String total_director;//累计总监人数
    public String total_manager;//累计部经理人数
    public String total_member;//累计专员人数
    public String new_manpower_month;//月入职人力
    public String new_manpower_quarter;//季度入职人力
    public String total_manpower;//累计总人力
    public String increase_quarter_plan;//季度增员目标
    public String activity_manpower_day;//日期交活动人力
    public String activity_manpower_month;//月期交活动人力
    public String enter_order_day;//日入职出单人力（期交)
    public String enter_order_month;//当月入职出单人力（期交)
    public String activity_rate_month;//月期交人员活动率
    public String eligible_manpower_day;//日合格人力
    public String eligible_manpower_month;//月合格人力
    public String eligible_manpower_ratio;//月合格人力占比
    public String productivity_month;//当月人均产能
    public String load_date;//(数据插入时间)
}
