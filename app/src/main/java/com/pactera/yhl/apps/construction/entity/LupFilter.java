package com.pactera.yhl.apps.construction.entity;

import lombok.Data;

import java.io.Serializable;

/**
 * @author: TSY
 * @create: 2021/11/26 0026 上午 10:34
 * @description:  用于获取lcpol和lbpol需要的字段
 */
@Data
public class LupFilter implements Serializable {
    public String polno;//保单险种号码
    public String edorno;//批单号
    public String contno;//合同号码
    public String contplancode;//保险计划编码
    public String mainpolno;//主险保单号码
    public String riskcode;//险种编码
    public String payendyear;//终交年龄年期
    public String signdate;//签单日期
    public String agentcode;//代理人编码
    public String prem;//保费
    public String mark;//用于标识lcpol或lbpol

    public String payintv;//交费间隔
    public String insuyear;//保险年龄年期
    public String payendyearflag;//终交年龄年期标志
    public String insuyearflag;//保险年龄年期标志
}
