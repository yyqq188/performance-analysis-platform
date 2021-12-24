package com.pactera.yhl.entity.source2;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProductConfig{
    public String product_code;
    public String product_payintv;
    public String product_name;
}
