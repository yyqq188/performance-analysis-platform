package com.pactera.yhl.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 16:11
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Lcappnt implements KLEntity {
    public String grpcontno;
    public String contno;
    public String prtno;
    public String appntno;
    public String appntgrade;
    public String appntname;
    public String appntsex;
    public String appntbirthday;
    public String appnttype;
    public String addressno;
    public String idtype;
    public String idno;
    public String nativeplace;
    public String nationality;
    public String rgtaddress;
    public String marriage;
    public String marriagedate;
    public String health;
    public String stature;
    public String avoirdupois;
    public String degree;
    public String creditgrade;
    public String bankcode;
    public String bankaccno;
    public String accname;
    public String joincompanydate;
    public String startworkdate;
    public String position;
    public String salary;
    public String occupationtype;
    public String occupationcode;
    public String worktype;
    public String pluralitytype;
    public String smokeflag;
    public String operator;
    public String managecom;
    public String makedate;
    public String maketime;
    public String modifydate;
    public String modifytime;
    public String bmi;
    public String othidtype;
    public String othidno;
    public String englishname;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String op_ts;
    public String current_ts;
    public String load_date;

    @Override
    public String toString() {
        return "{\"Lcappnt\":{"
                + "\"grpcontno\":\""
                + grpcontno + '\"'
                + ",\"contno\":\""
                + contno + '\"'
                + ",\"prtno\":\""
                + prtno + '\"'
                + ",\"appntno\":\""
                + appntno + '\"'
                + ",\"appntgrade\":\""
                + appntgrade + '\"'
                + ",\"appntname\":\""
                + appntname + '\"'
                + ",\"appntsex\":\""
                + appntsex + '\"'
                + ",\"appntbirthday\":\""
                + appntbirthday + '\"'
                + ",\"appnttype\":\""
                + appnttype + '\"'
                + ",\"addressno\":\""
                + addressno + '\"'
                + ",\"idtype\":\""
                + idtype + '\"'
                + ",\"idno\":\""
                + idno + '\"'
                + ",\"nativeplace\":\""
                + nativeplace + '\"'
                + ",\"nationality\":\""
                + nationality + '\"'
                + ",\"rgtaddress\":\""
                + rgtaddress + '\"'
                + ",\"marriage\":\""
                + marriage + '\"'
                + ",\"marriagedate\":\""
                + marriagedate + '\"'
                + ",\"health\":\""
                + health + '\"'
                + ",\"stature\":\""
                + stature + '\"'
                + ",\"avoirdupois\":\""
                + avoirdupois + '\"'
                + ",\"degree\":\""
                + degree + '\"'
                + ",\"creditgrade\":\""
                + creditgrade + '\"'
                + ",\"bankcode\":\""
                + bankcode + '\"'
                + ",\"bankaccno\":\""
                + bankaccno + '\"'
                + ",\"accname\":\""
                + accname + '\"'
                + ",\"joincompanydate\":\""
                + joincompanydate + '\"'
                + ",\"startworkdate\":\""
                + startworkdate + '\"'
                + ",\"position\":\""
                + position + '\"'
                + ",\"salary\":\""
                + salary + '\"'
                + ",\"occupationtype\":\""
                + occupationtype + '\"'
                + ",\"occupationcode\":\""
                + occupationcode + '\"'
                + ",\"worktype\":\""
                + worktype + '\"'
                + ",\"pluralitytype\":\""
                + pluralitytype + '\"'
                + ",\"smokeflag\":\""
                + smokeflag + '\"'
                + ",\"operator\":\""
                + operator + '\"'
                + ",\"managecom\":\""
                + managecom + '\"'
                + ",\"makedate\":\""
                + makedate + '\"'
                + ",\"maketime\":\""
                + maketime + '\"'
                + ",\"modifydate\":\""
                + modifydate + '\"'
                + ",\"modifytime\":\""
                + modifytime + '\"'
                + ",\"bmi\":\""
                + bmi + '\"'
                + ",\"othidtype\":\""
                + othidtype + '\"'
                + ",\"othidno\":\""
                + othidno + '\"'
                + ",\"englishname\":\""
                + englishname + '\"'
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
