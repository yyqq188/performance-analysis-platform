package entity;

import java.math.BigDecimal;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 16:39
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class imcont {
    public String data_source;
    public String branch_no;
    public String sales_id;
    public String plcid;
    public String apladdr;
    public String aplname;
    public String aplphone;
    public String outbid;
    public String pay_kind;
    public String bankagentid;
    public String aplidno;
    public String hobproflag;
    public String askgrpcontno;
    public String dutycount;
    public String agentcodeisnet;
    public String agentcomisnet;
    public String policyflag;
    public BigDecimal amnt;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String op_ts;
    public String current_ts;
    public String load_date;
}
