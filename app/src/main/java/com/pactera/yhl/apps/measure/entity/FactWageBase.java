package com.pactera.yhl.apps.measure.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Sun Haitian
 * @Description
 * @create 2021/11/20 12:21
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class FactWageBase {
    public String key_id;                                // 主键
    public String day_id;                               // 统计日期
    public String month_id;                              // 月份
    public String quarterly_id;                          // 季度


    //人员表
    public String workarea;                              // 作业地区
    public String hire_date;                             // 入司日期
    public String hire_month;                            // 入司年月
    public String agent_id;                             // 代理人编码
    public String agent_name;                             // 代理人姓名
    public String agentstate;                            // 代理人状态
    public String agent_grade;                            // 代理人职级

    public String to_agent_grade;                         // 代理人考核后职级

    //团队表
    public String department_code;                       // 部代码
    public String department_name;                       // 部名称
    public String department_agentcode;                  // 部经理代码
    public String department_leader;                     // 部经理名称
    public String district_code;                         // 区代码
    public String district_name;                         // 区名称
    public String district_agentcode;                    // 总监代码
    public String district_leader;                       // 总监名称

    //机构表
    public String provincecom_code;                      // 分公司代码
    public String provincecom_name;                      // 分公司名称

    public String assessmonth;                           // 考核月份
    public String d_fyp;                                 // 个人日标准保费
    public String m_fyp;                                 // 个人月度标准保费
    public String q_fyp;                                 // 个人季度标准保费
    public String member_rolling_rate;                   // 个人继续率
    public String is_manpower;                           // 是否达到活动人力

    public String department_d_fyp;                      // 所辖部日标准保费
    public String department_m_fyp;                      // 所辖部月度标准保费
    public String department_q_fyp;                      // 所辖部季度标准保费
    public String department_assessment;                 // 时点所辖部季度考核人力

    public String department_assessment_m;               // 所辖部月度考核人力

    public String department_activity_m;                 // 所辖部月度活动人力
    public String department_activity_q;                 // 所辖部季度活动人力
    public String department_activity_rate_m;            // 所辖部月度人员活动率
    public String department_activity_rate_q;            // 所辖部季度人员活动率
    public String department_rolling_rate;               // 所辖部继续率

    public String distinct_d_fyp;                        // 所辖区日标准保费
    public String distinct_m_fyp;                        // 所辖区月度标准保费
    public String distinct_q_fyp;                        // 所辖区季度标准保费
    public String distinct_assessment;                   // 时点所辖区季度考核人力

    public String distinct_assessment_m;                 // 所辖区月度考核人力

    public String distinct_activity_m;                   // 所辖区月度活动人力
    public String distinct_activity_q;                   // 所辖区季度活动人力
    public String distinct_activity_rate_m;              // 所辖区月度人员活动率
    public String distinct_activity_rate_q;              // 所辖区季度人员活动率
    public String distinct_rolling_rate;                 // 所辖区继续率

    public String month_num;                             // 季度所在月数
    public String assessment_standard;                   // 考核维持标准
    public String assessment_difference;                 // 任务差(目标 - 月保费)
    public String assessment_result;                         // 考核结果 1 2 3 4
    public String is_fullsalary;                         // 是否底薪全额获得 Y1 N0
    public String is_zero;                               // 是否挂零 Y1 N0

    public String load_date;                             // 加载日期
}
