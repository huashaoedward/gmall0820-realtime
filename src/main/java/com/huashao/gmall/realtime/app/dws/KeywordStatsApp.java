package com.huashao.gmall.realtime.app.dws;

import com.huashao.gmall.realtime.app.func.KeywordUDTF;
import com.huashao.gmall.realtime.bean.KeywordStats;
import com.huashao.gmall.realtime.common.GmallConstant;
import com.huashao.gmall.realtime.utils.ClickHouseUtil;
import com.huashao.gmall.realtime.utils.MyKafkaUtil;
import javassist.compiler.ast.Keyword;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Author: huashao
 * Date: 2021/8/11
 * Desc: dWS层 搜索关键字：从DWD层的页面日志中的商品列表页面读取数据，对搜索关键字分词；
 *          给定时间窗口内，哪个关键字，被搜索了多少次
 */
public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 创建Flink流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        /*
        //1.3 检查点CK相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop202:8020/gmall/flink/checkpoint/ProductStatsApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","atguigu");
        */
        //1.4 创建Table环境，使用建造者设计模式
        EnvironmentSettings setting = EnvironmentSettings
            .newInstance()
            .inStreamingMode()
            .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, setting);

        //TODO 2.注册自定义函数，注册了才能使用
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);

        //TODO 3.创建动态表
        //3.1 声明主题以及消费者组
        //数据源是DWD层的dwd_page_log
        String pageViewSourceTopic = "dwd_page_log";
        String groupId = "keywordstats_app_group";
        //3.2建表
        /*
        *  ！！注意common和page是MAP类型，因为它对应的是json字符串中的一个{},里面有很多KV对。里面的STRING也要大写;
        * 水位线是使用flink内置的时间转换函数TO_TIMESTAMP(),将yyyy-MM-dd HH:mm:ss格式的String，转为表中的timestamp;
        * 而使用FROM_UNIXTIME()，可以把String类型的时间戳的秒数，转为yyyy-MM-dd HH:mm:ss格式的String；
        * 乱序度为2秒；
        * */
        tableEnv.executeSql(
            "CREATE TABLE page_view (" +
                " common MAP<STRING, STRING>," +
                " page MAP<STRING, STRING>," +
                " ts BIGINT," +
                " rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                " WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND) " +
                " WITH (" + MyKafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId) + ")"
        );
        //TODO 4.从动态表中查询数据  --->尚硅谷大数据数仓-> [尚, 硅谷, 大, 数据, 数, 仓]
        /*page这个json里的item字段，对应值就是关键字；而page['item']是page列的map的的item这个key对应的值，
        并取别名为fullword;
        page['page_id']要是商品列表，才是搜索的关键字。
        */
        Table fullwordTable = tableEnv.sqlQuery(
            "select page['item'] fullword,rowtime " +
                " from page_view " +
                " where page['page_id']='good_list' and page['item'] IS NOT NULL"
        );
        //TODO 5.利用自定义函数  对搜索关键词进行拆分
        /*
        ik_analyze是注册的函数，返回的是一张表，表名是t,它里面有keyword这个列，列里是拆分好的关键词；
        rowtime是fullwordTable表中的列
         */
        Table keywordTable = tableEnv.sqlQuery(
            "SELECT keyword, rowtime " +
                "FROM  " + fullwordTable + "," +
                "LATERAL TABLE(ik_analyze(fullword)) AS t(keyword)"
        );
        //TODO 6.分组、开窗、聚合
        /*
        可统计出在各个窗口时间内的关键词出现的次数，统计时间
        count(*)即统计出关键词次数；
        DATE_FORMAT()是内置函数；转换窗口起始时间
        ‘SEARCH是一个常量字符串，可放入表中的列；因为group by后，可以select常量。
        以窗口和keyword分组；
        开窗后有窗口起始时间，统计时间
        最终结果就是，从DWD层的页面日志中的商品列表页面读取数据，对搜索关键字分词；哪个关键字，在哪个时段，被搜索了多少次
        */
        Table reduceTable = tableEnv.sqlQuery(
            "select keyword,count(*) ct,  '" + GmallConstant.KEYWORD_SEARCH + "' source," +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt ," +
                "UNIX_TIMESTAMP()*1000 ts from " + keywordTable +
                " group by TUMBLE(rowtime, INTERVAL '10' SECOND),keyword"
        );

        //TODO 7.转换为流
        //注意这里的泛型使用的是自定义的bean，KeywordStats
        DataStream<KeywordStats> keywordStatsDS = tableEnv.toAppendStream(reduceTable, KeywordStats.class);

        keywordStatsDS.print(">>>>");

        //TODO 8.写入到ClickHouse
        keywordStatsDS.addSink(
                /*
                如果bean的属性的顺序和clickHouse的建表定义的属性的顺序一致，sql就可以省略(keyword,ct,
                source,stt,edt,ts)这些属性名；如果不一致，就要加这些属性名。
                 */
            ClickHouseUtil.getJdbcSink("insert into keyword_stats_0820(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)")
        );

        env.execute();
    }
}
