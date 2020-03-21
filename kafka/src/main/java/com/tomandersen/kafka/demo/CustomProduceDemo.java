package com.tomandersen.kafka.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;

import java.util.Properties;

/**
 * <h3>KafkaProducer使用练习</h3>
 * <p>
 * 此类KafkaProducer的练习类,相关配置可以查看ProducerConfig,主要是用于练习Producer以及带回调
 * 函数callBack的Producer练习
 *
 * @date 2020/3/17
 * @see ProducerConfig
 * @see KafkaProducer
 * @see DefaultPartitioner
 */
public class CustomProduceDemo {

    public static void main(String[] args) {

        // 1.设置kafka producer相关配置信息
        // 这些配置信息都是org.apache.kafka.clients.producer.ProducerConfig类中属性,可以查看源码中对应的Javadoc介绍
        // 当然也可以直接调用此类中的类变量代替字符串Key
        Properties props = new Properties();
        // 设置kafka集群服务器主机
        props.put("bootstrap.servers", "hadoop101:9092,hadoop102:9092,hadoop103:9092");
        // 应答级别,可以为0 || 1 || all,all即为等到所有Partition的Follower的复制ack之后才继续生产下一条消息
        props.put("acks", "all");
        // 返回消息传递成功与否信息的最大等待时间,默认为2分钟
        props.put("delivery.timeout.ms", 120000);
        // 批次数据最大size,单位为Byte
        props.put("batch.size", 16384);
        // 批处理周期,即人为添加延迟增加每批次数据量,以减少发送请求次数
        props.put("linger.ms", 1);
        // 用于存放发送消息的缓冲区最大size
        props.put("buffer.memory", 33554432);
        // <K,V>序列工具
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        // 设置使用自定义的Partitioner,即根据规则判断发向的Partition,而不是在创建ProducerRecord时手动指定
//        props.put("partitioner.class", "com.tomandersen.kafka.partitioner.CustomPartitioner");
//        // Kafka自带默认的Partitioner,根据key的字节数组创建32bit hash,并映射到所有Partition中
//        props.put("partitioner.class","org.apache.kafka.clients.producer.internals.DefaultPartitioner");


        // 2.定义生产者对象
        Producer<String, String> kafkaProducer = null;

        // 3.调用Producer.send方法发送数据,kafka数据结构为ProducerRecord
        try {
            // 创建生产者Producer实例
            kafkaProducer = new KafkaProducer<>(props);
            // 循环发送数据
            for (int i = 0; i < 10; i++) {
                // 封装ProducerRecord并设置回调类CallBack,实现其onCompletion方法,这是发送结束的回调方法
                // 创建ProducerRecord时需要注意,默认是使用Hash的方式选择Partition
                // 即默认情况下Key值相同的消息会被分配到同一个Partition中
                kafkaProducer.send(new ProducerRecord<>("first",
                        Integer.toString(i + 1), Integer.toString(i)), (metadata, exception) -> {
                    // 如果Exception为空,则表示发送成功
                    if (exception == null) {
                        // 输出对应的Topic/Partition/Offset信息
                        System.out.println(metadata.topic() + "---" +
                                metadata.partition() + "---" + metadata.offset());
                    } else {
                        // 如果Exception为非空,则表示发送失败
                        System.out.println("发送失败");
                    }
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 4.关闭Producer
            if (kafkaProducer != null) kafkaProducer.close();
        }
    }
}
