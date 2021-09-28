package entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 16:04
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Laagentcertif {
    public String agency_sales_id;
    public String bank_id;
    public String channel_id;
    public String agency_sales_name;
    public String develop_card;
    public String developcard_start_date;
    public String developcard_end_date;
    public String insert_data;
    public String stateflag;
    public String idno;
    public String phone;
    public String mobile;
    public String etl_dt;
    public String etl_tm;
    public String etl_fg;
    public String op_ts;
    public String current_ts;
    public String load_date;
}
