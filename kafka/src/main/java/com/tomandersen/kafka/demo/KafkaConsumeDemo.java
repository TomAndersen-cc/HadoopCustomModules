package com.tomandersen.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * <h3>此类为使用KafkaConsumer使用练习</h3>
 * <p>
 * 具体例子可查看KafkaConsumer中的Javadoc,配置信息可查看ConsumerConfig
 *
 * @author TomAndersen
 * @version 1.0
 * @see KafkaConsumer
 * @see ConsumerConfig
 */
public class KafkaConsumeDemo {

    public static void main(String[] args) {
        // 1.配置信息
        Properties props = new Properties();
        // 设置kafka集群服务器主机
        props.put("bootstrap.servers", "hadoop101:9092");
        // 指定Consumer所属的ConsumerGroup
        props.put("group.id", "test");
        // 设置如果没有注册过当前ConsumerGroup则自动将此Consumer对应的Offset重置为最开始earliest,默认为latest
        // (由于Partition中数据会定期标记清除所以此值不一定为0)
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 设置每次获取完消息就自动提交Offset,当需要手动提交Offset时就需要将此参数设置成false
        props.put("enable.auto.commit", "false");
        // 设置自动提交Offset的延迟,为了减少Offset提交次数
        // 如果在等待过程中宕机而未成功提交Offset下次启动Consumer时则会出现消息的重复消费
        props.put("auto.commit.interval.ms", "1000");
        // 设置键值对<K,V>反序列化器,直接使用String对象反序列化器
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 设置取Consumer的Partition选择策略的partition.assignment.strategy
        // props.put("partition.assignment.strategy","com.tomandersen.kafka.partitioner.CustomPartitioner");

        KafkaConsumer<String, String> kafkaConsumer = null;
        try {
            // 2.创建消费者实例
            kafkaConsumer = new KafkaConsumer<>(props);
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

            // 4.按批次消费消息
            // 设置批次大小为200 Bytes
            final int batchSize = 200;
            // 设置等待消息的时间为10000ms
            final long delay = 10000L;
            // 获取当前系统时间
            long currentTime = System.currentTimeMillis();
            // 设置缓冲区
//        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
            // 缓冲区只用于存放需要获取的消息,此处将获取消息组成字符串输出
            List<String> buffer = new ArrayList<>();
            // 循环消费消息
            while (true) {
                // 每1000ms获取一次消息
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                // 将获取到的消息存入缓冲区
                for (ConsumerRecord record : records) {
                    // 每次获取topic/partition/offset/value信息
                    buffer.add(record.topic()
                            + "-" + record.partition()
                            + "-" + record.offset()
                            + "-" + record.value());
                }
                // 如果缓冲区达到批次大小或者非空却到达延迟时长,则输出消息,并刷新缓冲区
                if (buffer.size() >= batchSize ||
                        (buffer.size() != 0 && System.currentTimeMillis() - currentTime >= delay)) {
                    // 此处为具体的消费手段
                    // 可以打印到控制台,或者存入数据库等等
                    System.out.println(buffer);

                    // 向Kafka集群提交Offset
                    kafkaConsumer.commitAsync();
                    // 刷新缓冲区
                    buffer.clear();
                    // 刷新计时器
                    currentTime = System.currentTimeMillis();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (kafkaConsumer != null) kafkaConsumer.close();
        }
    }
}
