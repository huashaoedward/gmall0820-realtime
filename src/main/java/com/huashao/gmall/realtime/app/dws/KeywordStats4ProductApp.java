package com.huashao.gmall.realtime.app.dws;

import com.huashao.gmall.realtime.app.func.KeywordProductC2RUDTF;
import com.huashao.gmall.realtime.app.func.KeywordUDTF;
import com.huashao.gmall.realtime.bean.KeywordStats;
import com.huashao.gmall.realtime.utils.ClickHouseUtil;
import com.huashao.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Author: huashao
 * Date: 2021/8/14
 * Desc:  从商品主题中读取数据源，将spu_name中的关键词拆分，统计出给定时间窗口内，哪个词分别统计被点击，加购，下单的次数
 */
public class KeywordStats4ProductApp {
    public static void main(String[] args) throws Exception {
        //TODO 0.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);
        /*
        //CK相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop202:8020/gmall/flink/checkpoint/ProvinceStatsSqlApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","atguigu");
        */
        //TODO 1.定义Table流环境，建造者模式
        EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .inStreamingMode()
            .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //TODO 2.注册自定义函数
        //注册分词器的函数
        tableEnv.createTemporarySystemFunction("ik_analyze",  KeywordUDTF.class);
        //注册自定义的表函数，统计行为
        tableEnv.createTemporarySystemFunction("keywordProductC2R",  KeywordProductC2RUDTF.class);

        //TODO 3.将数据源定义为动态表
        String groupId = "keyword_stats_app";
        //从dws层的这个商品主题dws_product_stats中获取数据
        String productStatsSourceTopic ="dws_product_stats";

        //创建表
        tableEnv.executeSql("CREATE TABLE product_stats (spu_name STRING, " +
            "click_ct BIGINT," +
            "cart_ct BIGINT," +
            "order_ct BIGINT ," +
            "stt STRING,edt STRING ) " +
            "  WITH ("+ MyKafkaUtil.getKafkaDDL(productStatsSourceTopic,groupId)+")");

        //TODO 6.聚合计数
        //分词函数和统计的使用，
        /*
        侧写Lateral Table(<TableFunction>),
        会将外部表中的每一行，与表函数（TableFunction，算子的参数是它的表达式）计算得到的所有行连接起来。
        传入ik_analyze()的是spu_name，传入keywordProductC2R的是三个次数，这些参数都是product_stats这张表的列名。
        T(keyword)是分词返回结果表的别名，里面有一列keyword;
        T2(ct,source)是统计返回结果表别名，里面有两列ct和source。
        最终结果就是，从商品主题中读取数据源，将spu_name中的关键词拆分，哪个词分别统计被点击，加购，下单的次数
         */
        Table keywordStatsProduct = tableEnv.sqlQuery("select keyword,ct,source, " +
            "DATE_FORMAT(stt,'yyyy-MM-dd HH:mm:ss')  stt," +
            "DATE_FORMAT(edt,'yyyy-MM-dd HH:mm:ss') as edt, " +
            "UNIX_TIMESTAMP()*1000 ts from product_stats  , " +
            "LATERAL TABLE(ik_analyze(spu_name)) as T(keyword) ," +
            "LATERAL TABLE(keywordProductC2R( click_ct ,cart_ct,order_ct)) as T2(ct,source)");

        //TODO 7.转换为数据流
        DataStream<KeywordStats> keywordStatsProductDataStream =
            tableEnv.<KeywordStats>toAppendStream(keywordStatsProduct, KeywordStats.class);

        keywordStatsProductDataStream.print();
        //TODO 8.写入到ClickHouse
        keywordStatsProductDataStream.addSink(
            ClickHouseUtil.<KeywordStats>getJdbcSink(
                "insert into keyword_stats_0820(keyword,ct,source,stt,edt,ts)  " +
                    "values(?,?,?,?,?,?)"));

        env.execute();

    }
}
