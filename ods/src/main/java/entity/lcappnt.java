package entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 16:11
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class lcappnt {
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
    public String operator;
    public String managecom;
    public String makedate;
    public String maketime;
    public String modifydate;
    public String modifytime;
    public BigDecimal bmi;
    public String othidtype;
    public String othidno;
    public String englishname;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String op_ts;
    public String current_ts;
    public String load_date;

}
