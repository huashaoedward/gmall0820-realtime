package com.huashao.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.huashao.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Author: huashao
 * Date: 2021/7/28
 * Desc: 准备用户行为日志（日志数据）的DWD层
 */
public class BaseLogApp {
    private static final String TOPIC_START = "dwd_start_log";
    private static final String TOPIC_DISPLAY = "dwd_display_log";
    private static final String TOPIC_PAGE = "dwd_page_log";

    public static void main(String[] args) throws Exception {
        //TODO 1.准备环境
        //1.1 创建Flink流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2设置并行度, 并行度要和Kafka的分区数一致
        env.setParallelism(1);

        //1.3设置Checkpoint
        //每5000ms开始一次checkpoint，模式是EXACTLY_ONCE（默认）
        //env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //检查点超时时间
        //env.getCheckpointConfig().setCheckpointTimeout(60000);
        //设置状态后端，存入文件系统hdfs
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/checkpoint/baselogApp"));

        //用Idea代码往hdfs上写数据时，用户是administrator，会有permission denied
        //System.setProperty("HADOOP_USER_NAME","atguigu");

        //TODO 2.从Kafka中读取数据
        String topic = "ods_base_log";
        String groupId = "base_log_app_group";

        //2.1 调用Kafka工具类，获取FlinkKafkaConsumer
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //TODO 3.对读取到的数据格式进行转换         把日志从String格式，转为json对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(
            new MapFunction<String, JSONObject>() {
                @Override
                public JSONObject map(String value) throws Exception {
                    /*
                    Flink在重写方法时，map()的形参已经是从consumerRecord拿出的value中的消息体，
                    而spark要自己用consumerRecord.value拿出消息体。
                     */
                    //使用FastJson的parseObject直接把String转成Json对象
                    JSONObject jsonObject = JSON.parseObject(value);
                    return jsonObject;
                }
            }
        );
         //jsonObjDS.print("json>>>>>>>>");
        /*
        TODO 4.识别新老访客     前端也会对新老状态进行记录，有可能会不准，咱们这里是再次做一个确认
            保存mid某天方法情况（将首次访问日期作为状态保存起来），等后面该设备在有日志过来的时候，从状态中获取日期
            和日志产生日期进行对比。如果状态不为空，并且状态日期和当前日期不相等，说明是老访客，如果is_new标记是1，那么对其状态进行修复
        */
        //4.1 根据mid对日志进行分组
        /*
        这里keyBy传入确定的字段，mid，这样后续的processFunction的key的泛型就确定了，不会是java元组
         */
        KeyedStream<JSONObject, String> midKeyedDS = jsonObjDS.keyBy(
                //日志data的其中一个json是common，common中有一个字段是mid
            data -> data.getJSONObject("common").getString("mid")
        );

        //4.2 新老方法状态修复   状态分为算子状态和键控状态，我们这里要记录某一个设备的访问，使用键控状态比较合适
        //使用的是map算子来处理，里面是富函数
        SingleOutputStreamOperator<JSONObject> jsonDSWithFlag = midKeyedDS.map(
            new RichMapFunction<JSONObject, JSONObject>() {
                //定义该mid访问状态
                private ValueState<String> firstVisitDateState;
                //定义日期格式化对象
                private SimpleDateFormat sdf;

                @Override
                public void open(Configuration parameters) throws Exception {
                    //对状态以及日期格式进行初始化
                    firstVisitDateState = getRuntimeContext().getState(
                        new ValueStateDescriptor<String>("newMidDateState", String.class)
                    );
                    //将"yyyyMMdd"格式的String，转成日期格式，再转成时间戳的String
                    sdf = new SimpleDateFormat("yyyyMMdd");
                }

                @Override
                public JSONObject map(JSONObject jsonObj) throws Exception {
                    //获取当前日志标记状态，is_new字段，是在数据中的common这个json中的is_new字段
                    String isNew = jsonObj.getJSONObject("common").getString("is_new");

                    //获取当前日志访问时间戳，ts在最外层，不包含在common中
                    Long ts = jsonObj.getLong("ts");

                    //如果在日志数据中拿出是isNew字段是新客户，要再识别; 如果isNew字段是0，则一定是老客户；
                    //新客户可能会有误判，老客户则一定不会误判
                    if ("1".equals(isNew)) {
                        //获取当前mid对象的状态
                        String stateDate = firstVisitDateState.value();
                        //对当前条日志的"yyyyMMdd"格式的时间进行转换，ts是String，先转成date
                        // ，再转成String，格式自定义
                        String curDate = sdf.format(new Date(ts));
                        //如果状态不为空，并且状态日期和当前日期不相等，说明是老访客
                        if (stateDate != null && stateDate.length() != 0) {
                            //判断是否为同一天数据，不在同一天内访问，则是老用户
                            if (!stateDate.equals(curDate)) {
                                //标志不是新客户
                                isNew = "0";
                                //不是新客户，把最新的字段is_new更新
                                jsonObj.getJSONObject("common").put("is_new", isNew);
                            }
                        } else {
                            //如果还没记录设备的状态，将当前访问日志作为状态值
                            firstVisitDateState.update(curDate);
                        }
                    }
                    //返回 修复后的jsonObj
                    return jsonObj;
                }
            }
        );

        //jsonDSWithFlag.print(">>>>>>>>>>>");

        //TODO 5 .分流  根据日志数据内容,将日志数据分为3类, 页面日志、启动日志和曝光日志。
        // 页面日志输出到主流,启动日志输出到启动侧输出流,曝光日志输出到曝光日志侧输出流
        // 侧输出流：1)接收迟到数据    2)分流

        //定义启动侧输出流标签
        OutputTag<String> startTag = new OutputTag<String>("start"){};
        //定义曝光侧输出流标签
        OutputTag<String> displayTag = new OutputTag<String>("display"){};

        //自定义ProcessFunction
        SingleOutputStreamOperator<String> pageDS = jsonDSWithFlag.process(
            new ProcessFunction<JSONObject, String>() {
                @Override
                public void processElement(JSONObject jsonObj, Context ctx, Collector<String> out) throws Exception {
                    //获取启动日志标记
                    JSONObject startJsonObj = jsonObj.getJSONObject("start");
                    //将json格式转换为字符串，方便向侧输出流输出以及向kafka中写入
                    String dataStr = jsonObj.toString();

                    //判断是否为启动日志
                    if (startJsonObj != null && startJsonObj.size() > 0) {
                        //如果是启动日志，输出到启动侧输出流
                        ctx.output(startTag, dataStr);
                    } else {
                        //如果不是启动日志  说明是页面日志 ，输出到主流;
                        // ！！！页面日志包含有曝光的页面，和没曝光的页面日志；因为曝光肯定也是在某个页面上曝光的。
                        out.collect(dataStr);

                        //如果不是启动日志，获取曝光日志标记（曝光日志中也携带了页面）
                        //获取Json数组getJSONArray()
                        JSONArray displays = jsonObj.getJSONArray("displays");
                        //判断是否为曝光日志
                        if (displays != null && displays.size() > 0) {
                            //如果是曝光日志，遍历Json数组，输出到侧输出流
                            for (int i = 0; i < displays.size(); i++) {
                                //获取每一条曝光事件
                                JSONObject displaysJsonObj = displays.getJSONObject(i);
                                //获取页面id
                                String pageId = jsonObj.getJSONObject("page").getString("page_id");
                                //给每一条曝光事件加pageId
                                displaysJsonObj.put("page_id", pageId);
                                ctx.output(displayTag, displaysJsonObj.toString());
                            }
                        }

                    }
                }
            }
        );

        //获取侧输出流
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);

        //打印输出
        pageDS.print("page>>>>");
        startDS.print("start>>>>");
        displayDS.print("display>>>>");

        //TODO 6.将不同流的数据写回到kafka的不同topic中
        //kafka的sink写到工具类，再在这里调用
        FlinkKafkaProducer<String> startSink = MyKafkaUtil.getKafkaSink(TOPIC_START);
        startDS.addSink(startSink);

        FlinkKafkaProducer<String> displaySink = MyKafkaUtil.getKafkaSink(TOPIC_DISPLAY);
        displayDS.addSink(displaySink);

        FlinkKafkaProducer<String> pageSink = MyKafkaUtil.getKafkaSink(TOPIC_PAGE);
        pageDS.addSink(pageSink);


        env.execute();
    }
}
