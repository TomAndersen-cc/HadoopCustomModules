package com.tomandersen.kafka.demo;

import com.tomandersen.kafka.consumer.ConsumerRunner;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * <h3>此类主要用于测试自定义实现的ConsumerRunner能够正常使用</h3>
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/3/14
 * @see ConsumerRunner
 * @see ConsumerConfig
 */
public class KafkaConsumerRunnerDemo {

    public static void main(String[] args) {
        // 1.配置Consumer信息
        Properties props = new Properties();
        // 设置kafka集群服务器主机
        props.put("bootstrap.servers", "hadoop101:9092");
        // 指定Consumer所属的ConsumerGroup
        props.put("group.id", "test");
        // 设置如果没有注册过当前组自动将此Consumer对应的Offset重置为最开始earliest,默认为latest
        // (由于Partition中数据会定期标记清除所以此值不一定为0)
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 设置每次获取完消息就自动提交Offset,当需要自定义Offset提交时就需要将此参数设置成false
        props.put("enable.auto.commit", "false");
        // 设置自动提交Offset的延迟,为了减少Offset提交次数
        // 如果在等待过程中宕机而未成功提交Offset下次启动Consumer时则会出现消息的重复消费
        props.put("auto.commit.interval.ms", "1000");
        // 设置键值对<K,V>反序列化器,因为都是String类型,所以使用String对象反序列化器
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 设置取Consumer的Partition选择策略的partition.assignment.strategy
        // props.put("partition.assignment.strategy","com.tomandersen.kafka.partitioner.CustomPartitioner");

        // 2.创建消费者实例
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);

        // 3.订阅Topic
        // 方法1:使用subscribe(Collection<String>)订阅任意个数主题
        kafkaConsumer.subscribe(Collections.singletonList("first"));
        // 也可以同时订阅多个Topic
        /*kafkaConsumer.subscribe(Arrays.asList("first", "second"));*/

        // 方法2:使用subscribe(Pattern)订阅所有满足正则表达式的主题
            /*Pattern pattern = Pattern.compile("test.*");
            kafkaConsumer.subscribe(pattern);*/

        // 方法3:使用assign(Collection<TopicPartition>)订阅指定Topic的指定Partition
            /*TopicPartition topicPartition1 = new TopicPartition("first", 0);
            TopicPartition topicPartition2 = new TopicPartition("first", 1);
            kafkaConsumer.assign(Arrays.asList(topicPartition1, topicPartition2));*/

        // 创建消费者启动器ConsumerRunner,每批次最多消费10个消息,每次消费最长周期为10000ms
        ConsumerRunner consumerRunner = new ConsumerRunner(kafkaConsumer, 10, 10000);
        // 启动消费者线程consumerRunner
        new Thread(consumerRunner).start();
    }
}
