package com.me.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * 带回调的api
 */
public class MyProducerCallback {
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
            //带回调的API
            Future future = producer.send(new ProducerRecord("second", "atguigu+++" + i), new Callback() {

                /**
                 * 消息发送完成会执行该方法
                 * @param metadata  消息发送成功,metadata会存储当前消息的元数据信息
                 * @param exception 消息发送失败,exception中会有相应的异常信息
                 */
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        System.out.println("当前消息发送失败!!!!!!");
                    } else {
                        System.out.println(metadata.topic() + " -- " +
                                metadata.partition() + " -- " +
                                metadata.offset());
                    }
                }
            });
            System.out.println("=======消息发送完成=======");
        }

        // 休眠
        //Thread.sleep(1000);
        //TimeUnit.SECONDS.sleep(1);
        //TimeUnit.MILLISECONDS.sleep(1000);

        //3. 关闭生产者对象
        producer.close();

    }
}
