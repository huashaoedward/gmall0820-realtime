package com.huashao.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import com.huashao.gmall.realtime.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Author: huashao
 * Date: 2021/8/3
 * Desc: 从Phoenix中查询数据，将查询到每一条记录封装成指定的bean对象，再存入List中；返回值是List
 */
public class PhoenixUtil {
    private static Connection conn = null;

    //初始化方法
    public static void init(){
        try {
            //注册驱动
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            //获取Phoenix的连接
            conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            //指定操作的表空间
            conn.setSchema(GmallConfig.HABSE_SCHEMA);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 从Phoenix中查询数据
     * 询格式：select * from 表 where XXX=xxx
     * 还是使用泛型，和泛型模板<T>
     * @param sql
     * @param clazz  指定返回结果要封装的类型
     * @param <T>
     * @return  返回值是List，里面元素由传入的泛型决定
     */
    public static <T> List<T> queryList(String sql,Class<T> clazz){
        //如果连接为空，才初始化
        if(conn == null){
            init();
        }
        //List用于接收返回结果
        List<T> resultList = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            //获取数据库操作对象
            ps = conn.prepareStatement(sql);
            //执行SQL语句
            rs = ps.executeQuery();
            //通过结果集对象获取元数据信息
            ResultSetMetaData metaData = rs.getMetaData();
            //处理结果集
            while (rs.next()){
                //声明一个对象，用于封装查询的一条结果集
                T rowData = clazz.newInstance();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    //使用的是commons beanUtils把查询到的列数据封装入rowData对象属性
                    BeanUtils.setProperty(rowData,metaData.getColumnName(i),rs.getObject(i));
                }
                resultList.add(rowData);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从维度表中查询数据失败");
        } finally {
            //释放资源
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        return resultList;
    }

    public static void main(String[] args) {
        System.out.println(queryList("select * from DIM_BASE_TRADEMARK", JSONObject.class));
    }

}
