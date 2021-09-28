package entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 16:13
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Lcinsured implements KLEntity{
    public String grpcontno;
    public String contno;
    public String insuredno;
    public String prtno;
    public String appntno;
    public String managecom;
    public String executecom;
    public String familyid;
    public String relationtomaininsured;
    public String relationtoappnt;
    public String addressno;
    public String sequenceno;
    public String name;
    public String sex;
    public String birthday;
    public String idtype;
    public String idno;
    public String nativeplace;
    public String nationality;
    public String rgtaddress;
    public String marriage;
    public String marriagedate;
    public String health;
    public BigDecimal stature;
    public BigDecimal avoirdupois;
    public String degree;
    public String creditgrade;
    public String bankcode;
    public String bankaccno;
    public String accname;
    public String joincompanydate;
    public String startworkdate;
    public String position;
    public BigDecimal salary;
    public String occupationtype;
    public String occupationcode;
    public String worktype;
    public String pluralitytype;
    public String smokeflag;
    public String contplancode;
    public String operator;
    public String insuredstat;
    public String makedate;
    public String maketime;
    public String modifydate;
    public String modifytime;
    public String uwflag;
    public String uwcode;
    public String uwdate;
    public String uwtime;
    public BigDecimal bmi;
    public Integer insuredpeoples;
    public BigDecimal contplancount;
    public String diskimportno;
    public String othidtype;
    public String othidno;
    public String englishname;
    public String grpinsuredphone;
    public String medicalinsflag;
    public String destination;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String op_ts;
    public String current_ts;
    public String load_date;
}
