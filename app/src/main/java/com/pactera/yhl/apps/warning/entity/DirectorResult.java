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
public class DirectorResult {
    public String key_id;
    public String day_id;
    public String manage_code;
    public String manage_name;
    public String director_num;
    public String star_director_num;
    public String experience_director_num;
    public String senior_director_num;
    public String middle_director_num;
    public String primary_director_num;
    public String load_date;
}
