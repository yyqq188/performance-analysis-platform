package com.pactera.yhl.transform.normal.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 17:01
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Laagent implements KLEntity {
    public String agentcode;
    public String agentgroup;
    public String managecom;
    public String password;
    public String entryno;
    public String name;
    public String sex;
    public String birthday;
    public String nativeplace;
    public String nationality;
    public String marriage;
    public String creditgrade;
    public String homeaddresscode;
    public String homeaddress;
    public String postaladdress;
    public String zipcode;
    public String phone;
    public String bp;
    public String mobile;
    public String email;
    public String marriagedate;
    public String idno;
    public String source;
    public String bloodtype;
    public String polityvisage;
    public String degree;
    public String graduateschool;
    public String speciality;
    public String posttitle;
    public String foreignlevel;
    public String workage;
    public String oldcom;
    public String oldoccupation;
    public String headship;
    public String recommendagent;
    public String business;
    public String salequaf;
    public String quafno;
    public String quafstartdate;
    public String quafenddate;
    public String devno1;
    public String devno2;
    public String retaincontno;
    public String agentkind;
    public String devgrade;
    public String insideflag;
    public String fulltimeflag;
    public String noworkflag;
    public String traindate;
    public String employdate;
    public String indueformdate;
    public String outworkdate;
    public String recommendno;
    public String cautionername;
    public String cautionersex;
    public String cautionerid;
    public String cautionerbirthday;
    public String approver;
    public String approvedate;
    public String assumoney;
    public String remark;
    public String agentstate;
    public String qualipassflag;
    public String smokeflag;
    public String rgtaddress;
    public String bankcode;
    public String bankaccno;
    public String operator;
    public String makedate;
    public String maketime;
    public String modifydate;
    public String modifytime;
    public String branchtype;
    public String trainperiods;
    public String branchcode;
    public String age;
    public String channelname;
    public String receiptno;
    public String idnotype;
    public String branchtype2;
    public String trainpassflag;
    public String emergentlink;
    public String emergentphone;
    public String retainstartdate;
    public String retainenddate;
    public String togaeflag;
    public String archievecode;
    public String prepareenddate;
    public String preparagrade;
    public String preparatype;
    public String wageversion;
    public String crs_check_status;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String isnet;
    public String op_ts;
    public String current_ts;
    public String load_date;

    @Override
    public String toString() {
        return "{\"Laagent\":{"
                + "\"agentcode\":\""
                + agentcode + '\"'
                + ",\"agentgroup\":\""
                + agentgroup + '\"'
                + ",\"managecom\":\""
                + managecom + '\"'
                + ",\"password\":\""
                + password + '\"'
                + ",\"entryno\":\""
                + entryno + '\"'
                + ",\"name\":\""
                + name + '\"'
                + ",\"sex\":\""
                + sex + '\"'
                + ",\"birthday\":\""
                + birthday + '\"'
                + ",\"nativeplace\":\""
                + nativeplace + '\"'
                + ",\"nationality\":\""
                + nationality + '\"'
                + ",\"marriage\":\""
                + marriage + '\"'
                + ",\"creditgrade\":\""
                + creditgrade + '\"'
                + ",\"homeaddresscode\":\""
                + homeaddresscode + '\"'
                + ",\"homeaddress\":\""
                + homeaddress + '\"'
                + ",\"postaladdress\":\""
                + postaladdress + '\"'
                + ",\"zipcode\":\""
                + zipcode + '\"'
                + ",\"phone\":\""
                + phone + '\"'
                + ",\"bp\":\""
                + bp + '\"'
                + ",\"mobile\":\""
                + mobile + '\"'
                + ",\"email\":\""
                + email + '\"'
                + ",\"marriagedate\":\""
                + marriagedate + '\"'
                + ",\"idno\":\""
                + idno + '\"'
                + ",\"source\":\""
                + source + '\"'
                + ",\"bloodtype\":\""
                + bloodtype + '\"'
                + ",\"polityvisage\":\""
                + polityvisage + '\"'
                + ",\"degree\":\""
                + degree + '\"'
                + ",\"graduateschool\":\""
                + graduateschool + '\"'
                + ",\"speciality\":\""
                + speciality + '\"'
                + ",\"posttitle\":\""
                + posttitle + '\"'
                + ",\"foreignlevel\":\""
                + foreignlevel + '\"'
                + ",\"workage\":\""
                + workage + '\"'
                + ",\"oldcom\":\""
                + oldcom + '\"'
                + ",\"oldoccupation\":\""
                + oldoccupation + '\"'
                + ",\"headship\":\""
                + headship + '\"'
                + ",\"recommendagent\":\""
                + recommendagent + '\"'
                + ",\"business\":\""
                + business + '\"'
                + ",\"salequaf\":\""
                + salequaf + '\"'
                + ",\"quafno\":\""
                + quafno + '\"'
                + ",\"quafstartdate\":\""
                + quafstartdate + '\"'
                + ",\"quafenddate\":\""
                + quafenddate + '\"'
                + ",\"devno1\":\""
                + devno1 + '\"'
                + ",\"devno2\":\""
                + devno2 + '\"'
                + ",\"retaincontno\":\""
                + retaincontno + '\"'
                + ",\"agentkind\":\""
                + agentkind + '\"'
                + ",\"devgrade\":\""
                + devgrade + '\"'
                + ",\"insideflag\":\""
                + insideflag + '\"'
                + ",\"fulltimeflag\":\""
                + fulltimeflag + '\"'
                + ",\"noworkflag\":\""
                + noworkflag + '\"'
                + ",\"traindate\":\""
                + traindate + '\"'
                + ",\"employdate\":\""
                + employdate + '\"'
                + ",\"indueformdate\":\""
                + indueformdate + '\"'
                + ",\"outworkdate\":\""
                + outworkdate + '\"'
                + ",\"recommendno\":\""
                + recommendno + '\"'
                + ",\"cautionername\":\""
                + cautionername + '\"'
                + ",\"cautionersex\":\""
                + cautionersex + '\"'
                + ",\"cautionerid\":\""
                + cautionerid + '\"'
                + ",\"cautionerbirthday\":\""
                + cautionerbirthday + '\"'
                + ",\"approver\":\""
                + approver + '\"'
                + ",\"approvedate\":\""
                + approvedate + '\"'
                + ",\"assumoney\":\""
                + assumoney + '\"'
                + ",\"remark\":\""
                + remark + '\"'
                + ",\"agentstate\":\""
                + agentstate + '\"'
                + ",\"qualipassflag\":\""
                + qualipassflag + '\"'
                + ",\"smokeflag\":\""
                + smokeflag + '\"'
                + ",\"rgtaddress\":\""
                + rgtaddress + '\"'
                + ",\"bankcode\":\""
                + bankcode + '\"'
                + ",\"bankaccno\":\""
                + bankaccno + '\"'
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
                + ",\"branchtype\":\""
                + branchtype + '\"'
                + ",\"trainperiods\":\""
                + trainperiods + '\"'
                + ",\"branchcode\":\""
                + branchcode + '\"'
                + ",\"age\":\""
                + age + '\"'
                + ",\"channelname\":\""
                + channelname + '\"'
                + ",\"receiptno\":\""
                + receiptno + '\"'
                + ",\"idnotype\":\""
                + idnotype + '\"'
                + ",\"branchtype2\":\""
                + branchtype2 + '\"'
                + ",\"trainpassflag\":\""
                + trainpassflag + '\"'
                + ",\"emergentlink\":\""
                + emergentlink + '\"'
                + ",\"emergentphone\":\""
                + emergentphone + '\"'
                + ",\"retainstartdate\":\""
                + retainstartdate + '\"'
                + ",\"retainenddate\":\""
                + retainenddate + '\"'
                + ",\"togaeflag\":\""
                + togaeflag + '\"'
                + ",\"archievecode\":\""
                + archievecode + '\"'
                + ",\"prepareenddate\":\""
                + prepareenddate + '\"'
                + ",\"preparagrade\":\""
                + preparagrade + '\"'
                + ",\"preparatype\":\""
                + preparatype + '\"'
                + ",\"wageversion\":\""
                + wageversion + '\"'
                + ",\"crs_check_status\":\""
                + crs_check_status + '\"'
                + ",\"etl_dt\":\""
                + etl_dt + '\"'
                + ",\"etl_tm\":\""
                + etl_tm + '\"'
                + ",\"etl_fg\":\""
                + etl_fg + '\"'
                + ",\"isnet\":\""
                + isnet + '\"'
                + ",\"op_ts\":\""
                + op_ts + '\"'
                + ",\"current_ts\":\""
                + current_ts + '\"'
                + ",\"load_date\":\""
                + load_date + '\"'
                + "}}";

    }
}
