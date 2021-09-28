package entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 16:49
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Ldcode implements KLEntity{
    public String codetype;
    public String code;
    public String codename;
    public String codealias;
    public String comcode;
    public String othersign;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String op_ts;
    public String current_ts;
    public String load_date;
}
