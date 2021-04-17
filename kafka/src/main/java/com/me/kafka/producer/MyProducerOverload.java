package com.me.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyProducerOverload {
    public static void main(String[] args) {
        //0. 创建配置对象
        Properties props = new Properties();

        //指定kafka的集群位置
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        //指定ack的级别
        props.put(ProducerConfig.ACKS_CONFIG,"all");
        //指定重试次数
        props.put(ProducerConfig.RETRIES_CONFIG,5);
        //指定batch的大小
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        //指定等待时间
        props.put(ProducerConfig.LINGER_MS_CONFIG,1);
        //指定RecordAccumulator缓冲区大小
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
        //指定key的序列化器
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        //指定value的序列化器
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        //指定使用的分区器
        //props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"xxxx") ;

        //1. 创建生产者对象
        KafkaProducer producer = new KafkaProducer<String,String>(props);

        //2. 生产数据
        for (int i = 0; i < 10; i++) {
            //没有指定分区和key，所以用的是粘性分区
//            producer.send(new ProducerRecord("second","atguigu=="+i));
            //指定key后，通过key的hash值对分区数取余得出分区号
//            producer.send(new ProducerRecord("second", "key"+i, "atguigu-->"+i));
            //指定分区号，key就没用了，会严格按照分区号执行
            producer.send(new ProducerRecord("second",1, null,"atguigu##"+i));
        }

        //3. 关闭生产者对象
        producer.close();

    }
}
