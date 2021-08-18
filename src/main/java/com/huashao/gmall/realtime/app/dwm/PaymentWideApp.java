package com.huashao.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.huashao.gmall.realtime.bean.OrderWide;
import com.huashao.gmall.realtime.bean.PaymentInfo;
import com.huashao.gmall.realtime.bean.PaymentWide;
import com.huashao.gmall.realtime.utils.DateTimeUtil;
import com.huashao.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

/**
 * Author: huashao
 * Date: 2021/8/6
 * Desc: 支付宽表处理程序，要关联订单宽表的信息
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境的准备
        //1.1 创建流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        /*
        //1.3 检查点相关的配置
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/checkpoint/paymentWide"));
        */

        //TODO 2. 从kafka的主题中读取数据
        //2.1 声明相关的主题以及消费者组
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        String groupId = "paymentwide_app_group";

        //2.1 读取支付数据
        FlinkKafkaConsumer<String> paymentInfoSource = MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId);
        DataStreamSource<String> paymentInfoJsonStrDS = env.addSource(paymentInfoSource);

        //2.2 读取订单宽表数据
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);
        DataStreamSource<String> orderWideJsonStrDS = env.addSource(orderWideSource);

        //TODO 3. 对读取到的数据进行结构的转换   jsonStr->PaymentInfo
        //3.1 转换支付流
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentInfoJsonStrDS.map(
            jsonStr -> JSON.parseObject(jsonStr, PaymentInfo.class)
        );
        //3.2 转换订单流，jsonStr->OrderWide
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideJsonStrDS.map(
            jsonStr -> JSON.parseObject(jsonStr, OrderWide.class)
        );

        //paymentInfoDS.print("pay>>>>>");
        //orderWideDS.print("orderWide>>>>");

        //TODO 4.设置Watermark以及提取事件时间字段，因为使用intervalJoin时会有乱序和延时，所以要用水位线
        //4.1 支付流的Watermark
        /*要指定水位线，就要明确事件时间的时间戳在哪里获取；当前流里的数据是PaymentInfo这个bean
        ，它的一个属性是callback_time，是String类型，所以还要调用 工具类的DateTimeUtil.toTs，转成long类型*/
        SingleOutputStreamOperator<PaymentInfo> paymentInfoWithWatermarkDS = paymentInfoDS.assignTimestampsAndWatermarks(
            WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<PaymentInfo>() {
                        @Override
                        public long extractTimestamp(PaymentInfo paymentInfo, long recordTimestamp) {
                            //需要将字符串的时间转换为毫秒数
                            return DateTimeUtil.toTs(paymentInfo.getCallback_time());
                        }
                    }
                )
        );
        //4.2 订单流的Watermark
        SingleOutputStreamOperator<OrderWide> orderWideWithWatermarkDS = orderWideDS.assignTimestampsAndWatermarks(
            WatermarkStrategy.<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<OrderWide>() {
                        @Override
                        public long extractTimestamp(OrderWide orderWide, long recordTimestamp) {
                            return DateTimeUtil.toTs(orderWide.getCreate_time());
                        }
                    }
                )
        );

        //TODO 5.对数据进行分组
        //5.1 支付流数据分组，按orderId分组
        KeyedStream<PaymentInfo, Long> paymentInfoKeyedDS = paymentInfoWithWatermarkDS.keyBy(PaymentInfo::getOrder_id);
        //5.2 订单宽表流数据分组，按orderId分组
        KeyedStream<OrderWide, Long> orderWideKeyedDS = orderWideWithWatermarkDS.keyBy(OrderWide::getOrder_id);

        //TODO 6.使用IntervalJoin关联两条流
        //用支付信息表去关联订单宽表
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoKeyedDS
            .intervalJoin(orderWideKeyedDS)
                //下界是30分钟，上界是0，因为不能没有订单而支付，没有预付
            .between(Time.seconds(-1800), Time.seconds(0))
            .process(
                    //ProcessJoinFunction函数对象，泛型是<IN1, IN2, OUT>
                new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context ctx, Collector<PaymentWide> out) throws Exception {
                        /*用Collector发送出数据，是PaymentWide对象，构造参数是paymentInfo,orderWide对象*/
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                }
            );

        paymentWideDS.print(">>>>");
        //TODO 7.将数据写到kafka的dwm层
        paymentWideDS.map(
                //把流中的paymentWide对象，转成Json字符串。才能往Kafka里写
            paymentWide->JSON.toJSONString(paymentWide)
        ).addSink(
            MyKafkaUtil.getKafkaSink(paymentWideSinkTopic)
        );

        env.execute();
    }
}
