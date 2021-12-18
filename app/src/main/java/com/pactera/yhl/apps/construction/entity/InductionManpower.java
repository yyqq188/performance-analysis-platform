package com.pactera.yhl.apps.construction.entity;

import lombok.Data;

import java.io.Serializable;

/**
 * @author: TSY
 * @create: 2021/11/15 0015 下午 16:09
 * @description:
 */
@Data
public class InductionManpower implements Serializable {
    public String key_id;//主键
    public String rank;//职级
}
