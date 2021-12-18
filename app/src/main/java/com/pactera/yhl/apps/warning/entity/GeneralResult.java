package com.pactera.yhl.apps.warning.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author SUN KI
 * @Description
 * @create 2021/11/16 14:22
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class GeneralResult {
    public String key_id;
    public String day_id;
    public String manage_code;
    public String manage_name;
    public String agent_grade;
    public String manpower_assessment_day;
    public String advance_day;
    public String keep_day;
    public String degrade_day;
    public String degrade_rate_day;
    public String load_date;
}
