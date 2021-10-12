package com.pactera.yhl.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 16:32
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Ldplan implements KLEntity {
    public String contplancode;
    public String contplanname;
    public String plantype;
    public String planrule;
    public String plansql;
    public String remark;
    public String operator;
    public String makedate;
    public String maketime;
    public String modifydate;
    public String modifytime;
    public Integer peoples3;
    public String managecom;
    public String salechnl;
    public String startdate;
    public String enddate;
    public String contplancode2;
    public String contplanname2;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String op_ts;
    public String current_ts;
    public String load_date;

    @Override
    public String toString() {
        return "{\"Ldplan\":{"
                + "\"contplancode\":\""
                + contplancode + '\"'
                + ",\"contplanname\":\""
                + contplanname + '\"'
                + ",\"plantype\":\""
                + plantype + '\"'
                + ",\"planrule\":\""
                + planrule + '\"'
                + ",\"plansql\":\""
                + plansql + '\"'
                + ",\"remark\":\""
                + remark + '\"'
                + ",\"operator\":\""
                + operator + '\"'
                + ",\"makedate\":\""
                + makedate + '\"'
                + ",\"maketime\":\""
                + maketime + '\"'
                + ",\"modifydate\":\""
                + modifydate + '\"'
                + ",\"modifytime\":\""
                + modifytime + '\"'
                + ",\"peoples3\":"
                + peoples3
                + ",\"managecom\":\""
                + managecom + '\"'
                + ",\"salechnl\":\""
                + salechnl + '\"'
                + ",\"startdate\":\""
                + startdate + '\"'
                + ",\"enddate\":\""
                + enddate + '\"'
                + ",\"contplancode2\":\""
                + contplancode2 + '\"'
                + ",\"contplanname2\":\""
                + contplanname2 + '\"'
                + ",\"etl_dt\":\""
                + etl_dt + '\"'
                + ",\"etl_tm\":\""
                + etl_tm + '\"'
                + ",\"etl_fg\":\""
                + etl_fg + '\"'
                + ",\"op_ts\":\""
                + op_ts + '\"'
                + ",\"current_ts\":\""
                + current_ts + '\"'
                + ",\"load_date\":\""
                + load_date + '\"'
                + "}}";

    }
}
