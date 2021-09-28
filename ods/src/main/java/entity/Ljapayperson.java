package entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 16:44
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Ljapayperson {
    public String polno;
    public Integer paycount;
    public String grpcontno;
    public String grppolno;
    public String contno;
    public String managecom;
    public String agentcom;
    public String agenttype;
    public String riskcode;
    public String agentcode;
    public String agentgroup;
    public String paytypeflag;
    public String appntno;
    public String payno;
    public String payaimclass;
    public String dutycode;
    public String payplancode;
    public BigDecimal sumduepaymoney;
    public BigDecimal sumactupaymoney;
    public Integer payintv;
    public String paydate;
    public String paytype;
    public String enteraccdate;
    public String confdate;
    public String lastpaytodate;
    public String curpaytodate;
    public String ininsuaccstate;
    public String approvecode;
    public String approvedate;
    public String approvetime;
    public String serialno;
    public String operator;
    public String makedate;
    public String maketime;
    public String getnoticeno;
    public String modifydate;
    public String modifytime;
    public String finstate;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String op_ts;
    public String current_ts;
    public String load_date;
}
