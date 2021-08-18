package com.huashao.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.huashao.gmall.realtime.bean.VisitorStats;
import com.huashao.gmall.realtime.utils.ClickHouseUtil;
import com.huashao.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

/**
 * Author: huashao
 * Date: 2021/8/7
 * Desc:  访客主题统计: 统计一个时间段内（10秒窗口），不同地区、渠道、版本、新老访客下的PV,UV,SV,用户跳出数，访问时长等
 *
 * 需要启动的服务
 * -logger.sh(Nginx以及日志处理服务)、zk、kafka
 * -BaseLogApp、UniqueVisitApp、UserJumpDetailApp、VisitorStatsApp
 * 执行流程分析
 * -模拟生成日志数据
 * -交给Nginx进行反向代理
 * -交给日志处理服务 将日志发送到kafka的ods_base_log
 * -BaseLogApp从ods层读取数据，进行分流，将分流的数据发送到kakfa的dwd(dwd_page_log)
 * -UniqueVisitApp从dwd_page_log读取数据，将独立访客明细发送到dwm_unique_visit
 * -UserJumpDetailApp从dwd_page_log读取数据，将页面跳出明细发送到dwm_user_jump_detail
 * -VisitorStatsApp
 * >从dwd_page_log读取数据，计算pv、持续访问时间、session_count
 * >从dwm_unique_visit读取数据，计算uv
 * >从dwm_user_jump_detail读取数据，计算页面跳出
 * >输出
 * >统一格式 合并
 * >分组、开窗、聚合
 * >将聚合统计的结果保存到ClickHouse OLAP数据库
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 设置流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
        /*
        //1.3 检查点CK相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop202:8020/gmall/flink/checkpoint/VisitorStatsApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","atguigu");
        */

        //TODO 2.从kafka主题中读取数据
        //2.1 声明读取的主题名以及消费者组
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        String groupId = "visitor_stats_app";

        //2.2 从dwd_page_log主题中读取日志数据
        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        DataStreamSource<String> pvJsonStrDS = env.addSource(pageViewSource);

        //2.3 从dwm_unique_visit主题中读取uv数据
        FlinkKafkaConsumer<String> uvSource = MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId);
        DataStreamSource<String> uvJsonStrDS = env.addSource(uvSource);

        //2.4 从dwm_user_jump_detail主题中读取跳出数据
        FlinkKafkaConsumer<String> userJumpSource = MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId);
        DataStreamSource<String> userJumpJsonStrDS = env.addSource(userJumpSource);

        //2.5 输出各流中的数据
        //pvJsonStrDS.print("pv>>>>>");
        //uvJsonStrDS.print("uv>>>>>");
        //userJumpJsonStrDS.print("userJump>>>>>");

        // TODO 3.对各个流的数据进行结构的转换  jsonStr->VisitorStats；4流转换成相同结构是为了union
        // VisitorStats里面的属性是4个维度和4个度量
        // 3.1 转换pv流，每收到一个数据，pv数计为1，以供累加
        SingleOutputStreamOperator<VisitorStats> pvStatsDS = pvJsonStrDS.map(
                //注意泛型，String是流中的数据类型，VisitorStats是要转成的类型
            new MapFunction<String, VisitorStats>() {
                @Override
                //map的返回值是VisitorStats
                public VisitorStats map(String jsonStr) throws Exception {
                    //将json格式字符串转换为json对象
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    VisitorStats visitorStats = new VisitorStats(
                        "",
                        "",
                        //取出JsonObj的common这个对象的vc字段，转为为VisitorStats的属性
                        jsonObj.getJSONObject("common").getString("vc"),//版本
                        jsonObj.getJSONObject("common").getString("ch"),//渠道
                        jsonObj.getJSONObject("common").getString("ar"),//地区
                        jsonObj.getJSONObject("common").getString("is_new"), //新老访客
                        0L,
                        1L,
                        0L,
                        0L,
                        //持续访问时间
                        jsonObj.getJSONObject("page").getLong("during_time"),
                        //统计时间
                        jsonObj.getLong("ts")
                    );
                    return visitorStats;
                }
            }
        );
        // 3.2 转换uv流，每收到一个数据，uv数计为1，以供累加
        SingleOutputStreamOperator<VisitorStats> uvStatsDS = uvJsonStrDS.map(
            new MapFunction<String, VisitorStats>() {
                @Override
                public VisitorStats map(String jsonStr) throws Exception {
                    //将json格式字符串转换为json对象
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    VisitorStats visitorStats = new VisitorStats(
                        "",
                        "",
                        jsonObj.getJSONObject("common").getString("vc"),
                        jsonObj.getJSONObject("common").getString("ch"),
                        jsonObj.getJSONObject("common").getString("ar"),
                        jsonObj.getJSONObject("common").getString("is_new"),
                        1L,
                        0L,
                        0L,
                        0L,
                        0L,
                        jsonObj.getLong("ts")
                    );
                    return visitorStats;
                }
            }
        );
        //3.3 转换sv流（Session_count）,每收到一个数据，sv数计为1，以供累加
        // 使用的是process，不是map算子，其实还是从dwd_page_log中获取数据，筛选条件是要last_page_id为空
        SingleOutputStreamOperator<VisitorStats> svStatsDS = pvJsonStrDS.process(
                //泛型是I,O，String是输入类型，VisitorStats是输出类型
            new ProcessFunction<String, VisitorStats>() {
                @Override
                public void processElement(String jsonStr, Context ctx, Collector<VisitorStats> out) throws Exception {
                    //将json格式字符串转换为json对象
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    //获取当前页面的lastPageId，如果上一页面id是空，才是新的Session，才能计1次
                    String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                    if (lastPageId == null || lastPageId.length() == 0) {
                        VisitorStats visitorStats = new VisitorStats(
                            "",
                            "",
                            jsonObj.getJSONObject("common").getString("vc"),
                            jsonObj.getJSONObject("common").getString("ch"),
                            jsonObj.getJSONObject("common").getString("ar"),
                            jsonObj.getJSONObject("common").getString("is_new"),
                            0L,
                            0L,
                            1L,
                            0L,
                            0L,
                            jsonObj.getLong("ts")
                        );
                        //使用Collector发送数据，而不是返回值
                        out.collect(visitorStats);
                    }
                }
            }
        );

        //3.4 转换跳出流，之前已经统计了页面的跳出,符合CEP的才会在流里.
        //每收到一个数据，uj数计为1，以供累加
        SingleOutputStreamOperator<VisitorStats> userJumpStatsDS = userJumpJsonStrDS.map(
            new MapFunction<String, VisitorStats>() {
                @Override
                public VisitorStats map(String jsonStr) throws Exception {
                    //将json格式字符串转换为json对象
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    VisitorStats visitorStats = new VisitorStats(
                        "",
                        "",
                        jsonObj.getJSONObject("common").getString("vc"),
                        jsonObj.getJSONObject("common").getString("ch"),
                        jsonObj.getJSONObject("common").getString("ar"),
                        jsonObj.getJSONObject("common").getString("is_new"),
                        0L,
                        0L,
                        0L,
                        1L,
                        0L,
                        jsonObj.getLong("ts")
                    );
                    return visitorStats;
                }
            }
        );

        //TODO 4. 将4条流合并到一起   注意：union只能合并结构相同的流
        //4条流的结构相同，泛型都是VisitorStats
        DataStream<VisitorStats> unionDS = pvStatsDS.union(uvStatsDS, svStatsDS, userJumpStatsDS);

        //TODO 5.设置Watermmark以及提取事件时间，设置水位线是为了后面的开窗
        /*
        * assignTimestampsAndWatermarks()方法要传入的是WatermarkStrategy；
        * WatermarkStrategy的静态方法forBoundedOutOfOrderness()，传入参数为乱序度，泛型是VisitorStats，
        * 返回的还是WatermarkStrategy；
        * WatermarkStrategy的方法withTimestampAssigner，要传入的是SerializableTimestampAssigner对象，
        * 这个对象的泛型是VisitorStats，方法返回的还是WatermarkStrategy；
        * SerializableTimestampAssigner对象的方法extractTimestamp，才是最终提取时间戳
        * */
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWatermarkDS = unionDS.assignTimestampsAndWatermarks(
            WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<VisitorStats>() {
                        @Override
                        public long extractTimestamp(VisitorStats visitorStats, long recordTimestamp) {
                            return visitorStats.getTs();
                        }
                    }
                )
        );

        //TODO 6.分组  按照地区、渠道、版本、新老访客维度进行分组，因为我们这里有4个维度，所以将它们封装为一个Tuple4
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedDS = visitorStatsWithWatermarkDS.keyBy(
                //keyBy里传入KeySelector对象，泛型是<IN, KEY>
            new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
                @Override
                public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                    //这里新建Tuple对象的方式，为Tuple4.of()
                    return Tuple4.of(
                        visitorStats.getAr(),
                        visitorStats.getCh(),
                        visitorStats.getVc(),
                        visitorStats.getIs_new()
                    );
                }
            }
        );

        //TODO 7.开窗，滚动窗口
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowDS = keyedDS.window(
            TumblingEventTimeWindows.of(Time.seconds(10))
        );

        //TODO 8.对窗口的数据进行聚合   聚合结束之后，需要补充统计的起止时间
        //对WindowedStream进行reduce，里面就有预聚合和窗口函数
        SingleOutputStreamOperator<VisitorStats> reduceDS = windowDS.reduce(
                //reduce算子时要传入：预聚合函数ReduceFunction，窗口函数ProcessWindowFunction
            new ReduceFunction<VisitorStats>() {
                @Override
                public VisitorStats reduce(VisitorStats stats1, VisitorStats stats2) throws Exception {
                    stats1.setPv_ct(stats1.getPv_ct() + stats2.getPv_ct());
                    stats1.setUv_ct(stats1.getUv_ct() + stats2.getUv_ct());
                    stats1.setSv_ct(stats1.getSv_ct() + stats2.getSv_ct());
                    stats1.setDur_sum(stats1.getDur_sum() + stats2.getDur_sum());
                    return stats1;
                }
            },
                //注意窗口函数的泛型IN输入, OUT输出, KEY, W窗口，
            new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                @Override
                public void process(Tuple4<String, String, String, String> tuple4, Context context, Iterable<VisitorStats> elements, Collector<VisitorStats> out) throws Exception {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    //elements为窗口内的VisitorStats的可迭代集合
                    for (VisitorStats visitorStats : elements) {
                        //获取窗口的开始时间，从窗口的上下文中获取窗口开始时间
                        //把Long类型时间，转成Date类型，再转成yyyy-MM-dd HH:mm:ss格式的string
                        String startDate = sdf.format(new Date(context.window().getStart()));
                        //获取窗口的结束时间
                        String endDate = sdf.format(new Date(context.window().getEnd()));
                        visitorStats.setStt(startDate);
                        visitorStats.setEdt(endDate);
                        //统计时间，就是当前的时间戳
                        visitorStats.setTs(new Date().getTime());
                        //使用Collector发送数据visitorStats
                        out.collect(visitorStats);
                    }
                }
            }
        );

        //reduceDS.print(">>>>>");

        //TODO 9.向Clickhouse中插入数据
        /*sql里有12个占位符，*/
        reduceDS.addSink(
            ClickHouseUtil.getJdbcSink("insert into visitor_stats_0820 values(?,?,?,?,?,?,?,?,?,?,?,?)")
        );

        env.execute();

    }
}
