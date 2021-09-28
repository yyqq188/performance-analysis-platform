package entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 16:07
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Anychatcont implements KLEntity{
    public String businessno;
    public String drsflag;
    public String checkdate;
    public String ischeck;
    public String checkresult;
    public String fcd;
    public String flag1;
    public String flag2;
    public String flag3;
    public String flag4;
    public String operator;
    public String makedate;
    public String maketime;
    public String modifydate;
    public String modifytime;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String op_ts;
    public String current_ts;
    public String load_date;
}
