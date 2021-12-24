package com.pactera.yhl.transform.normal.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 16:59
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Labranchgroup implements KLEntity {
    public String agentgroup;
    public String name;
    public String managecom;
    public String upbranch;
    public String branchattr;
    public String branchseries;
    public String branchtype;
    public String branchlevel;
    public String branchmanager;
    public String branchaddresscode;
    public String branchaddress;
    public String branchphone;
    public String branchfax;
    public String branchzipcode;
    public String founddate;
    public String enddate;
    public String endflag;
    public String fieldflag;
    public String state;
    public String branchmanagername;
    public String upbranchattr;
    public String branchjobtype;
    public String operator;
    public String makedate;
    public String maketime;
    public String modifydate;
    public String modifytime;
    public String branchtype2;
    public String astartdate;
    public String branchlevelkind;
    public String trusteeship;
    public String insideflag;
    public String branchstyle;
    public String comstyle;
    public String costcenter;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String op_ts;
    public String current_ts;
    public String load_date;

    @Override
    public String toString() {
        return "{\"Labranchgroup\":{"
                + "\"agentgroup\":\""
                + agentgroup + '\"'
                + ",\"name\":\""
                + name + '\"'
                + ",\"managecom\":\""
                + managecom + '\"'
                + ",\"upbranch\":\""
                + upbranch + '\"'
                + ",\"branchattr\":\""
                + branchattr + '\"'
                + ",\"branchseries\":\""
                + branchseries + '\"'
                + ",\"branchtype\":\""
                + branchtype + '\"'
                + ",\"branchlevel\":\""
                + branchlevel + '\"'
                + ",\"branchmanager\":\""
                + branchmanager + '\"'
                + ",\"branchaddresscode\":\""
                + branchaddresscode + '\"'
                + ",\"branchaddress\":\""
                + branchaddress + '\"'
                + ",\"branchphone\":\""
                + branchphone + '\"'
                + ",\"branchfax\":\""
                + branchfax + '\"'
                + ",\"branchzipcode\":\""
                + branchzipcode + '\"'
                + ",\"founddate\":\""
                + founddate + '\"'
                + ",\"enddate\":\""
                + enddate + '\"'
                + ",\"endflag\":\""
                + endflag + '\"'
                + ",\"fieldflag\":\""
                + fieldflag + '\"'
                + ",\"state\":\""
                + state + '\"'
                + ",\"branchmanagername\":\""
                + branchmanagername + '\"'
                + ",\"upbranchattr\":\""
                + upbranchattr + '\"'
                + ",\"branchjobtype\":\""
                + branchjobtype + '\"'
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
                + ",\"branchtype2\":\""
                + branchtype2 + '\"'
                + ",\"astartdate\":\""
                + astartdate + '\"'
                + ",\"branchlevelkind\":\""
                + branchlevelkind + '\"'
                + ",\"trusteeship\":\""
                + trusteeship + '\"'
                + ",\"insideflag\":\""
                + insideflag + '\"'
                + ",\"branchstyle\":\""
                + branchstyle + '\"'
                + ",\"comstyle\":\""
                + comstyle + '\"'
                + ",\"costcenter\":\""
                + costcenter + '\"'
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
