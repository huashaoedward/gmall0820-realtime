package com.huashao.gmall.realtime.app.func;

import com.huashao.gmall.realtime.common.GmallConstant;
import com.huashao.gmall.realtime.common.GmallConstant;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * Author: huashao
 * Date: 2021/8/14
 * Desc:  自定义表函数，UDTF 函数实现点击次数、订单次数、添加购物次数的统计。
 *          数据会从一行转为多行
 */
//注解 返回的数据类型是row，操作次数是BIGINT，操作类型是STRING
@FunctionHint(output = @DataTypeHint("ROW<ct BIGINT,source STRING>"))
public class KeywordProductC2RUDTF extends TableFunction<Row> {
    //约定的函数，不是重写。传入的3个参数是三个点击，加购，下单次数
    public void eval(Long clickCt, Long cartCt, Long orderCt) {

        //生成三行，分别是点击，加购，下单的
        if(clickCt>0L) {
            //每一行有2列，是次数和操作类型
            Row rowClick = new Row(2);
            //设置点击次数
            rowClick.setField(0, clickCt);
            //设置操作类型
            rowClick.setField(1, GmallConstant.KEYWORD_CLICK);
            //发送数据
            collect(rowClick);
        }
        if(cartCt>0L) {
            Row rowCart = new Row(2);
            rowCart.setField(0, cartCt);
            rowCart.setField(1, GmallConstant.KEYWORD_CART);
            collect(rowCart);
        }
        if(orderCt>0) {
            Row rowOrder = new Row(2);
            rowOrder.setField(0, orderCt);
            rowOrder.setField(1, GmallConstant.KEYWORD_ORDER);
            collect(rowOrder);
        }
    }
}

