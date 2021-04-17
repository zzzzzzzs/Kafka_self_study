package com.me.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

/**
 * 消费者
 */
public class MyConsumer {
    public static void main(String[] args) {
        //0. 创建配置对象
        Properties props = new Properties();
        // 指定kafka集群的位置
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        // 指定消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "atguigu-group1");

        // 指定是否自动提交offset
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // 提交offset的间隔
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");


        //指定kv的反序列化器
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

        //1. 创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        //2. 订阅主题
        ArrayList<String> topics = new ArrayList<String>();
        topics.add("atguigu");
        topics.add("first");
        topics.add("second");
        //也可以订阅不存在的
        topics.add("third");
        consumer.subscribe(topics);

        //3. 消费
        while (true){
            ConsumerRecords<String, String>  records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.topic() + " -- " +
                        record.partition() + " -- " +
                        record.offset()+" -- " +
                        record.key() + " -- " +
                        record.value());

            }
        }
    }
}
