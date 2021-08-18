package com.huashao.gmall.realtime.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * Author: huashao
 * Date: 2021/8/6
 * Desc: 支付实体类，是对应支付信息表的bean
 */
@Data
public class PaymentInfo {
    Long id;
    Long order_id;
    Long user_id;
    BigDecimal total_amount;
    String subject;
    String payment_type;
    String create_time;
    String callback_time;
}

