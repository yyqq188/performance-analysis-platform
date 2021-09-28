package entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 16:50
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Lpedoritem implements KLEntity{
    public String edoracceptno;
    public String edorno;
    public String edorappno;
    public String edortype;
    public String displaytype;
    public String grpcontno;
    public String contno;
    public String insuredno;
    public String polno;
    public String managecom;
    public String edorvalidate;
    public String edorappdate;
    public String edorstate;
    public String uwflag;
    public String uwoperator;
    public String uwdate;
    public String uwtime;
    public BigDecimal chgprem;
    public BigDecimal chgamnt;
    public BigDecimal getmoney;
    public BigDecimal getinterest;
    public String operator;
    public String makedate;
    public String maketime;
    public String modifydate;
    public String modifytime;
    public String edorreasonno;
    public String edorreason;
    public String reason;
    public String reasoncode;
    public String approvegrade;
    public String approvestate;
    public String approveoperator;
    public String approvedate;
    public String approvetime;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String op_ts;
    public String current_ts;
    public String load_date;
}