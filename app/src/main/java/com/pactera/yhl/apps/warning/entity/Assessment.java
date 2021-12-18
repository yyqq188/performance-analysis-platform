package com.pactera.yhl.apps.warning.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/11/18 14:57
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Assessment {
    public String sales_id;
    public double prem;
    public String flag;
    public String rank_flag;
    public int labor;
}
