package com.huashao.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.huashao.gmall.realtime.common.GmallConfig;
import com.huashao.gmall.realtime.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * 自定义Flink的sink, 写入到phoenix连接的Hbase中，它的输入的数据类型是 JSONObject
 * Author: huashao
 * Date: 2021/8/1
 * Desc:  写出维度数据的Sink实现类
 */
public class DimSink extends RichSinkFunction<JSONObject> {
    //定义Phoenix连接对象
    private Connection conn = null;

    //因为有Phoenix数据库连接，要在open方法中初始化，所以不能用普通的SinkFunction，要用富函数，才有open方法
    @Override
    public void open(Configuration parameters) throws Exception {
        //对连接对象进行初始化
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    /**
     * 对流中的数据进行处理：
     * 根据data中属性名和属性值，生成upsert语句，再通过Phoenix往Hbase写入数据；
     * 如果当前做的是更新操作，需要将Redis中缓存的旧数据清除掉（新的数据在被第一次查询到时再缓存）
     *
     * @param jsonObj
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        //获取目标表的名称，这个表也是在ProcessElement()方法中添加的字段
        String tableName = jsonObj.getString("sink_table");
        //获取json中data数据   data数据就是经过过滤之后  保留的业务表中字段
        JSONObject dataJsonObj = jsonObj.getJSONObject("data");

        if (dataJsonObj != null && dataJsonObj.size() > 0) {
            //根据data中属性名和属性值  生成upsert语句
            String upsertSql = genUpsertSql(tableName.toUpperCase(), dataJsonObj);
            System.out.println("向Phoenix插入数据的SQL:" + upsertSql);

            //执行SQL
            PreparedStatement ps = null;
            try {
                //根据sql生成预编译语句
                ps = conn.prepareStatement(upsertSql);
                ps.execute();
                //注意：执行完Phoenix插入操作之后，需要手动提交事务
                conn.commit();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException("向Phoenix插入数据失败");
            } finally {
                if (ps != null) {
                    ps.close();
                }
            }

            //如果当前做的是更新操作，需要将Redis中缓存的旧数据清除掉（新的数据在被第一次查询到时再缓存）
            if(jsonObj.getString("type").equals("update")||jsonObj.getString("type").equals("delete")){
                DimUtil.deleteCached(tableName,dataJsonObj.getString("id"));
            }
        }


    }

    /**
     * 根据表名和data属性和值  生成向Phoenix中插入数据的sql语句
     *
     * @param tableName  表名
     * @param dataJsonObj  传入的数据JSONObject
     * @return
     */
    private String genUpsertSql(String tableName, JSONObject dataJsonObj) {
        /*
            {
                "id":88,
                "tm_name":"xiaomi"
            }
        */
        //"upsert into 表空间.表名(列名.....) values (值....)"
        Set<String> keys = dataJsonObj.keySet();
        Collection<Object> values = dataJsonObj.values();

        //使用apache commons-utils中的StringUtils.join，用,把集合set中的元素连接起来
        String upsertSql = "upsert into " + GmallConfig.HABSE_SCHEMA + "." + tableName + "(" +
            StringUtils.join(keys, ",") + ")";

        String valueSql = " values ('" + StringUtils.join(values, "','") + "')";
        return upsertSql + valueSql;
    }
}
