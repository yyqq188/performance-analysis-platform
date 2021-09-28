package entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 15:52
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Ljtempfeeclass implements KLEntity{
    public String tempfeeno;
    public String paymode;
    public String chequeno;
    public String paymoney;
    public String appntname;
    public String paydate;
    public String confdate;
    public String approvedate;
    public String enteraccdate;
    public String confflag;
    public String serialno;
    public String managecom;
    public String policycom;
    public String bankcode;
    public String bankaccno;
    public String accname;
    public String confmakedate;
    public String confmaketime;
    public String chequedate;
    public String operator;
    public String makedate;
    public String maketime;
    public String modifydate;
    public String modifytime;
    public String insbankcode;
    public String insbankaccno;
    public String payperson;
    public String pubankcode;
    public String pubankaccno;
    public String puaccname;
    public String tempfeetype;
    public String otherno;
    public String othernotype;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String op_ts;
    public String current_ts;
    public String load_date;

}
