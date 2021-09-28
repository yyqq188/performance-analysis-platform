package entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/9/27 17:02
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class T01teaminfo implements KLEntity{
    public String channel_id;
    public String branch_id;
    public String workspace_id;
    public String team_id_parent;
    public String team_id;
    public String team_name;
    public String team_lvl;
    public String leader_id;
    public String leader_id_reserve;
    public String startup_date;
    public String stat;
    public String address;
    public String zipcode;
    public String telephone;
    public String fax;
    public String team_id_old;
    public String found_date;
    public String cancel_date;
    public String cancel_reason;
    public String sales_goal;
    public String remark;
    public String insert_time;
    public String user_id;
    public String operate_type;
    public String team_type;
    public String channel_id_2nd;
    public String leader_position_date;
    public String versionid;
    public String team_id_parent_old;
    public String team_lvl_old;
    public String leader_type;
    public String manager_name;
    public String team_name_system;
    public String is_inherency;
    public String reserve_date;
    public String op_ts;
    public String current_ts;
    public String load_date;
}
