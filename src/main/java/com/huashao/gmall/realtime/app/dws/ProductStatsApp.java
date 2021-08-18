package com.huashao.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.huashao.gmall.realtime.app.func.DimAsyncFunction;
import com.huashao.gmall.realtime.common.GmallConstant;
import com.huashao.gmall.realtime.bean.OrderWide;
import com.huashao.gmall.realtime.bean.PaymentWide;
import com.huashao.gmall.realtime.bean.ProductStats;
import com.huashao.gmall.realtime.utils.ClickHouseUtil;
import com.huashao.gmall.realtime.utils.DateTimeUtil;
import com.huashao.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * Author: huashao
 * Date: 2021/8/11
 * Desc: 商品主题统计应用：统计一个时间段内（10秒窗口），各个商品id的点击，曝光，收藏，加购，下单商品个数，下单金额，
 *       订单数，支付金额，支付单数，退款单数，评论数，好评数等各个度量
 *
 *       使用建造者模式给bean赋值也是重要点之一
 *
 * 执行前需要启动的服务
 *    -zk,kafka,logger.sh(nginx + 日志处理服务),maxwell,hdfs,hbase,Redis,ClichHouse
 *    -BaseLogApp,BaseDBApp,OrderWideApp,PaymentWide,ProductStatsApp
 * 执行流程
 */
public class ProductStatsApp {
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

        //TODO 2.从Kafka中获取数据流
        //2.1 声明相关的主题名称以及消费者组
        String groupId = "product_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String favorInfoSourceTopic = "dwd_favor_info";
        String cartInfoSourceTopic = "dwd_cart_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        //2.2 从页面日志中获取点击和曝光数据
        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);

        //2.3 从dwd_favor_info中获取收藏数据
        FlinkKafkaConsumer<String> favorInfoSourceSouce = MyKafkaUtil.getKafkaSource(favorInfoSourceTopic, groupId);
        DataStreamSource<String> favorInfoDStream = env.addSource(favorInfoSourceSouce);

        //2.4 从dwd_cart_info中获取购物车数据
        FlinkKafkaConsumer<String> cartInfoSource = MyKafkaUtil.getKafkaSource(cartInfoSourceTopic, groupId);
        DataStreamSource<String> cartInfoDStream = env.addSource(cartInfoSource);

        //2.5 从dwm_order_wide中获取订单数据
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);
        DataStreamSource<String> orderWideDStream = env.addSource(orderWideSource);

        //2.6 从dwm_payment_wide中获取支付数据
        FlinkKafkaConsumer<String> paymentWideSource = MyKafkaUtil.getKafkaSource(paymentWideSourceTopic, groupId);
        DataStreamSource<String> paymentWideDStream = env.addSource(paymentWideSource);

        //2.7 从dwd_order_refund_info中获取退款数据
        FlinkKafkaConsumer<String> refundInfoSource = MyKafkaUtil.getKafkaSource(refundInfoSourceTopic, groupId);
        DataStreamSource<String> refundInfoDStream = env.addSource(refundInfoSource);

        //2.8 从dwd_comment_info中获取评价数据
        FlinkKafkaConsumer<String> commentInfoSource = MyKafkaUtil.getKafkaSource(commentInfoSourceTopic, groupId);
        DataStreamSource<String> commentInfoDStream = env.addSource(commentInfoSource);


        //TODO 3.将各个流的数据转换为统一的对象格式，ProductStats对象，统一结构是为了union
        //3.1 对点击和曝光数据进行转换      jsonStr-->ProduceStats
        SingleOutputStreamOperator<ProductStats> productClickAndDispalyDS = pageViewDStream.process(
                //注意泛型是I,O
            new ProcessFunction<String, ProductStats>() {
                @Override
                public void processElement(String jsonStr, Context ctx, Collector<ProductStats> out) throws Exception {
                    //将json格式字符串转换为json对象
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                    String pageId = pageJsonObj.getString("page_id");
                    if (pageId == null) {
                        System.out.println(">>>>" + jsonObj);
                    }
                    //获取操作时间
                    Long ts = jsonObj.getLong("ts");
                    //如果当前访问的页面是商品详情页，即pageId是good_detail，认为该商品被点击了一次
                    if ("good_detail".equals(pageId)) {
                        //获取被点击商品的id
                        Long skuId = pageJsonObj.getLong("item");
                        /*封装一次点击操作，使用建造者模式为ProductStats的属性赋值，
                        就是把被点击的商品id和曝光次数1次,封装入ProductStats对象 */
                        ProductStats productStats = ProductStats.builder().sku_id(skuId).click_ct(1L).ts(ts).build();
                        //向下游输出
                        out.collect(productStats);
                    }

                    JSONArray displays = jsonObj.getJSONArray("displays");
                    //如果displays属性不为空，那么说明有曝光数据
                    if (displays != null && displays.size() > 0) {
                        //曝光数据是一个数组，对它遍历
                        for (int i = 0; i < displays.size(); i++) {
                            //获取曝光数据
                            JSONObject displayJsonObj = displays.getJSONObject(i);
                            //判断是否曝光的是某一个商品id
                            if ("sku_id".equals(displayJsonObj.getString("item_type"))) {
                                //获取商品id
                                Long skuId = displayJsonObj.getLong("item");
                                //封装曝光商品对象，就是把被曝光的商品id和曝光次数1次,封装入ProductStats对象
                                ProductStats productStats = ProductStats.builder().sku_id(skuId).display_ct(1L).ts(ts).build();
                                //向下游输出
                                out.collect(productStats);
                            }
                        }
                    }
                }
            }
        );

        //3.2 对订单宽表进行转换      实际是jsonStr先转成OrderWide，再转ProductStats
        //主要是把订单相关的信息，如商品id，下单商品个数,下单商品金额, 下单时间，封装入对象，之后可以对这几个度量累加
        SingleOutputStreamOperator<ProductStats> orderWideStatsDS = orderWideDStream.map(
                //泛型是I,O
            new MapFunction<String, ProductStats>() {
                @Override
                public ProductStats map(String jsonStr) throws Exception {
                    //将json字符串转换为对应的订单宽表对象OrderWide
                    OrderWide orderWide = JSON.parseObject(jsonStr, OrderWide.class);
                    String create_time = orderWide.getCreate_time();
                    //将字符串日期转换为毫秒数
                    Long ts = DateTimeUtil.toTs(create_time);
                    ProductStats productStats = ProductStats.builder()
                        .sku_id(orderWide.getSku_id())
                        .order_sku_num(orderWide.getSku_num())
                        .order_amount(orderWide.getSplit_total_amount())
                        .ts(ts)
                            //使用set来存放订单id，orderId；工具类Collections.singleton()
                            //这是从商品出发,一个商品可能对应多个订单  每个订单包含的商品数量不一样
                            // 所以先记录一下订单id 之后再做关联  因为之后要做关联 数据传输skuname都没有设置
                        .orderIdSet(new HashSet(Collections.singleton(orderWide.getOrder_id())))
                            //创建外部类并返回
                        .build();
                    return productStats;
                }
            }
        );

        //3.3转换收藏流数据，jsonStr先转成jsonObj，再转成ProductStats；
        //把商品id，被收藏1次，收藏时间封装入ProductStats对象
        SingleOutputStreamOperator<ProductStats> favorStatsDS = favorInfoDStream.map(
            new MapFunction<String, ProductStats>() {
                @Override
                public ProductStats map(String jsonStr) throws Exception {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    //将字符串日期转换为毫秒数
                    Long ts = DateTimeUtil.toTs(jsonObj.getString("create_time"));
                    ProductStats productStats = ProductStats.builder()
                        .sku_id(jsonObj.getLong("sku_id"))
                        .favor_ct(1L)
                        .ts(ts)
                        .build();
                    return productStats;
                }
            }
        );

        //3.4转换购物车流数据
        //把商品id，被收藏1次，收藏时间封装入ProductStats对象
        SingleOutputStreamOperator<ProductStats> cartStatsDS = cartInfoDStream.map(
            new MapFunction<String, ProductStats>() {
                @Override
                public ProductStats map(String jsonStr) throws Exception {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    //将字符串日期转换为毫秒数
                    Long ts = DateTimeUtil.toTs(jsonObj.getString("create_time"));

                    ProductStats productStats = ProductStats.builder()
                        .sku_id(jsonObj.getLong("sku_id"))
                        .cart_ct(1L)
                        .ts(ts)
                        .build();
                    return productStats;
                }
            }
        );

        //3.5转换支付流数据, 实际是jsonStr先转成PaymentWide，再转ProductStats
        //主要是把支付相关的信息，如商品id，支付商品金额, 支付订单的单号组成的集合，支付时间，封装入对象，之后可以对这几个度量累加
        SingleOutputStreamOperator<ProductStats> paymentStatsDS = paymentWideDStream.map(
            new MapFunction<String, ProductStats>() {
                @Override
                public ProductStats map(String jsonObj) throws Exception {
                    PaymentWide paymentWide = JSON.parseObject(jsonObj, PaymentWide.class);
                    Long ts = DateTimeUtil.toTs(paymentWide.getPayment_create_time());
                    return ProductStats.builder()
                        .sku_id(paymentWide.getSku_id())
                        .payment_amount(paymentWide.getSplit_total_amount())
                        .paidOrderIdSet(new HashSet(Collections.singleton(paymentWide.getOrder_id())))
                        .ts(ts)
                        .build();
                }
            }
        );

        //3.6转换退款流数据
        //主要是把退款相关的信息，如商品id，退款商品金额, 退款订单的单号组成的集合，时间，封装入对象，之后可以对这几个度量累加
        SingleOutputStreamOperator<ProductStats> refundStatsDS= refundInfoDStream.map(
            jsonStr -> {
                JSONObject refundJsonObj = JSON.parseObject(jsonStr);
                Long ts = DateTimeUtil.toTs(refundJsonObj.getString("create_time"));
                ProductStats productStats = ProductStats.builder()
                    .sku_id(refundJsonObj.getLong("sku_id"))
                    .refund_amount(refundJsonObj.getBigDecimal("refund_amount"))
                    .refundOrderIdSet(
                        new HashSet(Collections.singleton(refundJsonObj.getLong("order_id"))))
                    .ts(ts)
                    .build();
                return productStats;
            });

        //3.7转换评价流数据
        //把商品id，被评论1次，好评数，评论时间封装入ProductStats对象
        SingleOutputStreamOperator<ProductStats> commonInfoStatsDS= commentInfoDStream.map(
            jsonStr -> {
                JSONObject commonJsonObj = JSON.parseObject(jsonStr);
                Long ts = DateTimeUtil.toTs(commonJsonObj.getString("create_time"));
                //好评的转换，如果是好评，次数是1，否则是0
                Long goodCt = GmallConstant.APPRAISE_GOOD.equals(commonJsonObj.getString("appraise")) ? 1L : 0L;
                ProductStats productStats = ProductStats.builder()
                    .sku_id(commonJsonObj.getLong("sku_id"))
                    .comment_ct(1L)
                    .good_comment_ct(goodCt)
                    .ts(ts)
                    .build();
                return productStats;
            });

        //TODO 4. 将转换后的流进行合并
        DataStream<ProductStats> unionDS = productClickAndDispalyDS.union(
            orderWideStatsDS,
            favorStatsDS,
            cartStatsDS,
            paymentStatsDS,
            refundStatsDS,
            commonInfoStatsDS
        );

        //TODO 5.设置Watermark并且提取事件时间字段
        /*
         * assignTimestampsAndWatermarks()方法要传入的是WatermarkStrategy；
         * WatermarkStrategy的静态方法forMonotonousTimestamps()，是时间单调递增，不是乱序，返回的还是WatermarkStrategy；
         * WatermarkStrategy的方法withTimestampAssigner，要传入的是SerializableTimestampAssigner对象，
         * 这个对象的泛型是ProductStats，方法返回的还是WatermarkStrategy；
         * SerializableTimestampAssigner对象的方法extractTimestamp，才是最终提取时间戳
         * */
        SingleOutputStreamOperator<ProductStats> productStatsWithWatermarkDS = unionDS.assignTimestampsAndWatermarks(
                //时间单调递增，不是乱序
            WatermarkStrategy.<ProductStats>forMonotonousTimestamps()
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<ProductStats>() {
                        @Override
                        //指定哪个字段作为时间戳
                        public long extractTimestamp(ProductStats productStats, long recordTimestamp) {
                            return productStats.getTs();
                        }
                    }
                )
        );

        //TODO 6.按照维度对数据进行分组，商品主题以sku_id来分组
        KeyedStream<ProductStats, Long> keyedDS = productStatsWithWatermarkDS.keyBy(
            new KeySelector<ProductStats, Long>() {
                @Override
                public Long getKey(ProductStats productStats) throws Exception {
                    return productStats.getSku_id();
                }
            }
        );

        //TODO 7.对分组之后的数据进行开窗   开一个10s的滚动窗口
        WindowedStream<ProductStats, Long, TimeWindow> windowDS = keyedDS.window(
            TumblingEventTimeWindows.of(Time.seconds(10))
        );

        //TODO 8.对窗口中的元素进行聚合
        SingleOutputStreamOperator<ProductStats> reduceDS = windowDS.reduce(
                //预聚合函数
            new ReduceFunction<ProductStats>() {
                @Override
                public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                    //曝光，点击，加购，收藏次数的相加
                    stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                    stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                    stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                    stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                    //因为是BigDecimal类型，不能直接相加，要用add方法
                    stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                    //把第二个数据的set的元素，添加到第一个数据中，并会自动去重
                    stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                    //订单个数，集合里放的是订单号，所以集合长度就是订单数
                    // stats1.getOrderIdSet().size()返回的是int类型，加0L则转换为Long类型
                    stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                    //同一个窗口，同一个key 商品id的两个对象的下单商品个数相加
                    stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());

                    //同一个窗口，同一个key 商品id的两个对象的退款订单号的集合的合并
                    stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                    //退款单数，集合里放的是退款订单号，所以集合长度就是退款订单数
                    stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                    //同一个窗口，同一个key 商品id的两个对象的退款商品金额相加
                    stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                    //同一个窗口，同一个key 商品id的两个对象的支付订单号的集合的合并
                    stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                    //退款单数，集合里放的是支付订单号，所以集合长度就是支付订单数
                    stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);
                    //同一个窗口，同一个key 商品id的两个对象的支付商品金额相加
                    stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                    //同一个窗口，同一个key 商品id的两个对象的评论数相加
                    stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                    //同一个窗口，同一个key 商品id的两个对象的好评数相加
                    stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());

                    return stats1;
                }
            },
                //窗口函数，拿到窗口起始时间，统计时间
                //不用windowProcessFunction，以后可能会弃用；注意窗口函数的泛型IN输入, OUT输出, KEY, W窗口，
            new ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                @Override
                public void process(Long key, Context context, Iterable<ProductStats> elements, Collector<ProductStats> out) throws Exception {
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    for (ProductStats productStats : elements) {
                        //窗口起始时间
                        productStats.setStt(simpleDateFormat.format(new Date(context.window().getStart())));
                        productStats.setEdt(simpleDateFormat.format(new Date(context.window().getEnd())));
                        //统计时间
                        productStats.setTs(new Date().getTime());
                        out.collect(productStats);
                    }
                }
            }

        );

        //TODO 9.补充商品的维度信息
        //9.1 关联商品维度，异步查询
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(
            reduceDS,
            //注意泛型是ProductStats，关联表名是DIM_SKU_INFO
            new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                //查询的key，是商品id即sku_id
                @Override
                public String getKey(ProductStats productStats) {
                    return productStats.getSku_id().toString();
                }

                @Override
                public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws Exception {
                    //把关联查询到的维度信息，封装入ProductStats
                    productStats.setSku_name(dimInfoJsonObj.getString("SKU_NAME"));
                    productStats.setSku_price(dimInfoJsonObj.getBigDecimal("PRICE"));
                    productStats.setSpu_id(dimInfoJsonObj.getLong("SPU_ID"));
                    productStats.setTm_id(dimInfoJsonObj.getLong("TM_ID"));
                    productStats.setCategory3_id(dimInfoJsonObj.getLong("CATEGORY3_ID"));
                }
            },
                //超时时间
            60,
            TimeUnit.SECONDS
        );
        //9.2 关联SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS = AsyncDataStream.unorderedWait(
            productStatsWithSkuDS,
                //注意泛型是ProductStats，关联表名是DIM_SPU_INFO
            new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                @Override
                //查询的key，是Spu_id
                public String getKey(ProductStats productStats) {
                    return productStats.getSpu_id().toString();
                }

                @Override
                //把关联到的Spu_name，赋值给ProductStats
                public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws Exception {
                    productStats.setSpu_name(dimInfoJsonObj.getString("SPU_NAME"));
                }
            },
            60,
            TimeUnit.SECONDS
        );

        //9.3 关联品牌维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTMDS = AsyncDataStream.unorderedWait(
            productStatsWithSpuDS,
                //注意泛型是ProductStats，关联表名是DIM_BASE_TRADEMARK
            new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                @Override
                //查询的key，是Tm_id
                public String getKey(ProductStats productStats) {
                    return productStats.getTm_id().toString();
                }

                @Override
                //把关联到的Tm_name，赋值给ProductStats
                public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws Exception {
                    productStats.setTm_name(dimInfoJsonObj.getString("TM_NAME"));
                }
            },
            60,
            TimeUnit.SECONDS
        );

        //9.4 关联品类维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategoryDS = AsyncDataStream.unorderedWait(
            productStatsWithTMDS,
                //注意泛型是ProductStats，关联表名是DIM_BASE_CATEGORY3
            new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                @Override
                //查询的key，是Category3_id
                public String getKey(ProductStats productStats) {
                    return productStats.getCategory3_id().toString();
                }

                @Override
                //把关联到的Category3_id，赋值给ProductStats
                public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws Exception {
                    productStats.setCategory3_name(dimInfoJsonObj.getString("NAME"));
                }
            },
            60,
            TimeUnit.SECONDS
        );

        productStatsWithCategoryDS.print(">>>>");

        //TODO 10.将聚合后的流数据写到ClickHouse中
        productStatsWithCategoryDS.addSink(
                //使用ClickHouseUtil工具类的方法
            ClickHouseUtil
                .<ProductStats>getJdbcSink("insert into product_stats_0820 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                ));

        //TODO 11.将统计的结果写回到kafka的dws层
        productStatsWithCategoryDS
                //将流中的数据productStat对象，转成json格式的String
                //JSON.toJSONString()方法要传入的第一个参数是要转的对象，第二个参数是？？
            .map(productStat->JSON.toJSONString(productStat,new SerializeConfig(true)))
            .addSink(MyKafkaUtil.getKafkaSink("dws_product_stats"));

        env.execute();

    }
}
