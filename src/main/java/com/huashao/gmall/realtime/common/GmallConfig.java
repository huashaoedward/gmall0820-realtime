package com.huashao.gmall.realtime.common;

/**
 * Author: huashao
 * Date: 2021/7/31
 * Desc: 项目配置的常量类
 */
public class GmallConfig {
    //Hbase的命名空间
    public static final String HABSE_SCHEMA = "GMALL0820_REALTIME";

    //Phonenix连接的服务器地址
    public static final String PHOENIX_SERVER="jdbc:phoenix:hadoop102," +
            "hadoop103,hadoop104:2181";

    //ClickHouse的URL连接地址
    public static final String CLICKHOUSE_URL="jdbc:clickhouse://hadoop102" +
            ":8123/default";

}
