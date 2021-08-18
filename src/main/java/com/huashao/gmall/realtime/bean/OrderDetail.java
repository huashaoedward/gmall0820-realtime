package com.huashao.gmall.realtime.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * Author: huashao
 * Date: 2021/8/3
 * Desc: 订单明细实体类
 */
//使用Lombok注解，不需要写get，set，构造等方法
@Data
public class OrderDetail {
    Long id;
    Long order_id ;
    Long sku_id;
    BigDecimal order_price ;
    Long sku_num ;
    String sku_name;
    String create_time;
    BigDecimal split_total_amount;
    BigDecimal split_activity_amount;
    BigDecimal split_coupon_amount;
    Long create_ts;
}

