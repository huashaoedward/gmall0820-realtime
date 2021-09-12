package com.huashao.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * Author: huashao
 * Date: 2021/7/28
 * Desc: 操作Kafka的工具类
 */
public class MyKafkaUtil {
    private static String KAFKA_SERVER = "hadoop102:9092,hadoop103:9092," +
            "hadoop104:9092";
    //默认主题
    private static String DEFAULT_TOPIC = "DEFAULT_DATA";


    //获取FlinkKafkaConsumer,因为是工具类，所以用静态
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {
        //Kafka连接的一些属性配置
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        //SimpleStringSchema是字符串类型作反序列化用的
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
    }

    //封装FlinkKafkaProducer
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<String>(KAFKA_SERVER, topic, new SimpleStringSchema());
    }



    /**
     * 根据序列化方式，获取Kafka Sink
     * 可指定序列化方式(之前只能是SimpleStringSchema，现在可传入泛型对象，或JSONObject)，
     * 而且因为不能指定topic，所以没法传入topic，所有数据发往指定默认的主题"DEFAULT_DATA"
     * @param kafkaSerializationSchema
     * @param <T>
     * @return
     */
    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        //设置生产数据的超时时间15分钟
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"");
        /**
         * FlinkKafkaProducer的4个参数：
         * topic,序列化结构，配置信息，消息语义
         */
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC, kafkaSerializationSchema, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    //拼接Kafka相关配置属性到DDL，主要是使用TABLE api时，在流数据中创建表时使用
    public static String getKafkaDDL(String topic,String groupId){
        String ddl="'connector' = 'kafka', " +
            " 'topic' = '"+topic+"',"   +
            " 'properties.bootstrap.servers' = '"+ KAFKA_SERVER +"', " +
            " 'properties.group.id' = '"+groupId+ "', " +
            "  'format' = 'json', " +
            "  'scan.startup.mode' = 'latest-offset'  ";
        return  ddl;
    }

}
