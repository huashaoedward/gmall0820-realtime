package com.huashao.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.huashao.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Author: HUASHAO
 * Date: 2021/8/2
 * Desc:  从kafka中的DWD层的事实表dwd_page_log读数据，同一天内重复的访客的去重，
 *        结果是一天内所有不重复的访客，但并没有计算出最终UV访客数目
 *
 *        重点： 1. keyBy 设备mid,以用户分组；
 *              2. 两个if，过滤同一用户同一session的不是第一页面的数据，过滤访问时间同一天的数据；
 *              3. 在状态描述器的配置参数里设置状态的有效期为1天。
 *
 * 前期准备：
 *   -启动ZK、Kafka、Logger.sh、BaseLogApp、UniqueVisitApp
 * 执行流程:
 *   模式生成日志的jar->nginx->日志采集服务->kafka(ods)
 *   ->BaseLogApp(分流)->kafka(dwd) dwd_page_log
 *   ->UniqueVisitApp(独立访客)->kafka(dwm_unique_visit)
 */
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1  准备本地测试流环境
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /*通过Flink的web ui页面就可以看到运行的本程序，不像之前要打包上传运行才能看到。
        需要加依赖，flink-runtime-web_2.12*/
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //1.2 设置并行度
        env.setParallelism(4);

        //1.3 设置Checkpoint
        //env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(60000);
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/checkpoint/uniquevisit"))

        //TODO 2.从kafka中读取数据
        //从dwd_page_log读取数据，dwd_page_log是日志数据分出的流，是页面数据
        String sourceTopic = "dwd_page_log";
        String groupId = "unique_visit_app_group";
        String sinkTopic = "dwm_unique_visit";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> jsonStrDS = env.addSource(kafkaSource);

        //TODO 3.对读取到的数据进行结构的换换， 由String转成jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = jsonStrDS.map(jsonStr -> JSON.parseObject(jsonStr));

        //TODO 4.按照设备id进行分组；
        //根据common这个Json里的mid这个字段分组，同一mid就是同一用户访问
        KeyedStream<JSONObject, String> keybyWithMidDS = jsonObjDS.keyBy(
            jsonObj -> jsonObj.getJSONObject("common").getString("mid")
        );
        //TODO 5.过滤得到UV
        SingleOutputStreamOperator<JSONObject> filteredDS = keybyWithMidDS.filter(
                //使用富函数，才有生命周期，才能在open里定义sdf，设置日期状态有效期为1天
            new RichFilterFunction<JSONObject>() {
                //定义状态，存入上一次访问页面的时间；
                ValueState<String> lastVisitDateState = null;
                //定义日期工具类
                SimpleDateFormat sdf = null;

                @Override
                public void open(Configuration parameters) throws Exception {
                    //初始化日期工具类
                    sdf = new SimpleDateFormat("yyyyMMdd");
                    //初始化状态，创建状态描述器
                    ValueStateDescriptor<String> lastVisitDateStateDes =
                        new ValueStateDescriptor<>("lastVisitDateState", String.class);
                    //因为我们统计的是日活DAU，所以状态数据只在当天有效 ，过了一天就可以失效掉
                    //建造者模式，行到StateTtlConfig对象
                    StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1)).build();
                    //在描述器中设置ttl
                    lastVisitDateStateDes.enableTimeToLive(stateTtlConfig);
                    //初始化状态，传入状态描述器
                    this.lastVisitDateState = getRuntimeContext().getState(lastVisitDateStateDes);
                }

                @Override
                public boolean filter(JSONObject jsonObj) throws Exception {

                    //留下同一用户同一session访问的第一个页面，过滤掉不是第一页面的所有页面。
                    String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                    if (lastPageId != null && lastPageId.length() > 0) {
                        //filter返回false，就是有上一个访问页面，当前数据不过滤，跳过，再处理下一条数据
                        return false;
                    }

                    //以下为lastPageId为空的情况，没上一个访问页面
                    //获取当前访问时间，ts字段是和其他几个jsonObj同一级并列的
                    Long ts = jsonObj.getLong("ts");
                    //将当前访问时间戳转换为日期字符串，ts转为Data,再转为yyyyMMdd格式的String
                    String logDate = sdf.format(new Date(ts));
                    //获取状态日期
                    String lastVisitDate = lastVisitDateState.value();

                    //用当前页面的访问时间和状态时间进行对比
                    if (lastVisitDate != null && lastVisitDate.length() > 0 && lastVisitDate.equals(logDate)) {
                        System.out.println("已访问：lastVisitDate-" + lastVisitDate + ",||logDate:" + logDate);
                        //当前页面访问时间和状态中的上一次访问同一页面的日期是相同的，则是同一天内访问同一页面，当前数据不过滤，跳过，再处理下一条数据
                        return false;
                    } else {
                        //当前页面访问时间和状态中的上一次访问同一页面的日期是不同，不是同一天访问同一页面
                        System.out.println("未访问：lastVisitDate-" + lastVisitDate + ",||logDate:" + logDate);
                        //在状态中记录最新数据的页面访问时间
                        lastVisitDateState.update(logDate);
                        return true;
                    }
                }
            }
        );

        //filteredDS.print(">>>>>");

        //TODO 6. 向kafka中写回，需要将json转换为String
        //6.1 json->string
        SingleOutputStreamOperator<String> kafkaDS = filteredDS.map(jsonObj -> jsonObj.toJSONString());

        //6.2 写回到kafka的dwm层
        kafkaDS.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        env.execute();
    }
}
