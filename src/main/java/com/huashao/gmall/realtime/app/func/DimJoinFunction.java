package com.huashao.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

/**
 * Author: huashao
 * Date: 2021/8/5
 * Desc:  维度关联接口:这个异步维表查询的方法适用于各种维表的查询，用什么条件查，查出来的结果如何合并到数据流对象中，需要使用者自己定义。
 *      重点是使用了模式设计方法
 */
public interface DimJoinFunction<T> {

    //需要提供一个获取key的方法，但是这个方法如何实现不知道
    String getKey(T obj);

    //流中的事实数据和查询出来的维度数据进行关联
    void join(T obj, JSONObject dimInfoJsonObj) throws Exception;
}
