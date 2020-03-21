package com.tomandersen.kafka.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

/**
 * <h3>自定义的Consume工具</h3>
 * <h>
 * 主要是通过封装KafkaConsumer中的API来实现几个自定义的功能.
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/3/17
 * @see KafkaConsumer
 */
public class CustomConsumes {
    // 保存指定的TopicPartition的所有副本,用于当Leader宕机后寻找新的Leader
    private static Node[] replications;


    /**
     * <p>
     * 用于寻找指定Brokers-Topic-Partition对应的Leader.
     * 主要是封装了{@link KafkaConsumer#partitionsFor}方法.
     *
     * @param brokers   指定的Kafka集群,格式为<"host","port">
     * @param topic     指定的Topic
     * @param partition 指定的Partition ID
     * @return Node {@link Node}
     * @date 2020/3/17
     */
    public static Node findLeader(Map<String, Integer> brokers, String topic, int partition) {
        // 1.通过接收到的配置信息创建KafkaConsumer实例
        Properties props = new Properties();
        for (String broker : brokers.keySet()) {
            props.put("bootstrap.servers",
                    broker + ":" + brokers.getOrDefault(broker, 9092));
        }
        // 指定Consumer所属的ConsumerGroup
        props.put("group.id", "test");
        // 设置手动提交Offset,由于只是获取部分数据所以不需要自动提交Offset
        props.put("enable.auto.commit", "false");
        // 设置键值对<K,V>反序列化器,使用String对象反序列化器
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 2.定义KafkaConsumer实例,采用try{}catch{}finally{}的方式获取资源
        KafkaConsumer<String, String> kafkaConsumer = null;
        try {
            // 3.创建KafkaConsumer实例
            kafkaConsumer = new KafkaConsumer<>(props);
            // 4.通过KafkaConsumer实例获取指定Topic对应的Partition信息
            List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topic);
            // 5.遍历返回的指定Topic的所有Partition信息
            for (PartitionInfo partitionInfo : partitionInfos) {
                // 若当前Partition是指定的Partition,则保存此Partition的Replication并将其Leader返回
                if (partitionInfo.partition() == partition) {
                    /*// 保存指定Partition的所有副本
                    replications = partitionInfo.replicas();*/
                    // 6.返回指定的TopicPartition对应的Leader
                    return partitionInfo.leader();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (kafkaConsumer != null) kafkaConsumer.close();
        }
        // 如果没有找到Leader则返回null
        return null;
    }


    /**
     * <p>根据指定的Topic-Partition获取对应的endOffset.
     * 主要是封装了KafkaConsumer的{@link KafkaConsumer#endOffsets}方法.
     * <p>endOffsets Javadoc:
     * Get the end offsets for the given partitions. In the default {@code read_uncommitted} isolation level, the end
     * offset is the high watermark (that is, the offset of the last successfully replicated message plus one). For
     * {@code read_committed} consumers, the end offset is the last stable offset (LSO), which is the minimum of
     * the high watermark and the smallest offset of any open transaction. Finally, if the partition has never been
     * written to, the end offset is 0.
     *
     * @param brokers   指定的Kafka集群
     * @param topic     指定的Topic
     * @param partition 指定的Partition ID
     * @return TopicPartition对应最新消息的Offset
     * @date 2020/3/17
     * @see KafkaConsumer#endOffsets
     */
    public static long getLastOffset(Map<String, Integer> brokers, String topic, int partition) {
        // 1.配置Consumer
        // 通过接收到的配置信息创建KafkaConsumer实例
        Properties props = new Properties();
        for (String broker : brokers.keySet()) {
            props.put("bootstrap.servers",
                    broker + ":" + brokers.getOrDefault(broker, 9092));
        }
        // 指定Consumer所属的ConsumerGroup
        props.put("group.id", "test");
        // 设置手动提交Offset,由于只是获取部分数据所以不需要自动提交Offset
        props.put("enable.auto.commit", "false");
        // 设置键值对<K,V>反序列化器,直接使用String对象反序列化器
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 2.定义消费者KafkaConsumer对象
        KafkaConsumer<String, String> kafkaConsumer = null;
        // 3.创建TopicPartition实例
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        try {
            // 4.创建kafkaTopic实例
            kafkaConsumer = new KafkaConsumer<>(props);
            // 5.通过Consumer.endOffsets(Collections<TopicPartition>)方法获取
            // 指定TopicPartition对应的lastOffset
            Map offsets = kafkaConsumer.endOffsets(
                    Collections.singletonList(topicPartition));
            // 6.返回对应Partition的Offset,如果不存在则直接返回0
            return (Long) offsets.getOrDefault(topicPartition, 0);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (kafkaConsumer != null) kafkaConsumer.close();
        }
        // 获取lastOffset失败则返回-1
        return -1;
    }


    /**
     * <p>主要用于从指定的Topic-Partition-Offset读取指定数量的消息.
     * 主要是用于获取部分数据,而不是持续消费数据.主要是封装了{@link KafkaConsumer#assign}
     * 和{@link KafkaConsumer#seek}方法.
     *
     * @param brokers     指定的Kafka集群
     * @param topic       指定消费的Topic
     * @param partition   指定消费的Partition ID
     * @param beginOffset 指定开始消费的Offset
     * @param timeInMills 指定消费的时长
     * @param maxReads    指定消费的最大消息数
     * @date 2020/3/17
     */
    public static List<ConsumerRecord> getRecordsFrom(Map<String, Integer> brokers, String topic, int partition, long beginOffset, long timeInMills, long maxReads) {
        // 1.通过接收到的配置信息创建KafkaConsumer实例
        Properties props = new Properties();
        for (String broker : brokers.keySet()) {
            props.put("bootstrap.servers",
                    broker + ":" + brokers.getOrDefault(broker, 9092));
        }
        // 设置手动提交Offset,由于只是获取部分数据所以不需要自动提交Offset
        props.put("enable.auto.commit", "false");
        // 设置键值对<K,V>反序列化器,因为都是String类型,所以使用String对象反序列化器
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 2.定义KafkaConsumer对象,以及创建用于返回的ConsumerRecord集合
        KafkaConsumer<Object, Object> kafkaConsumer = null;
        List<ConsumerRecord> recordList = new ArrayList<>();
        try {
            // 3.创建KafkaConsumer实例
            kafkaConsumer = new KafkaConsumer<>(props);
            // 4.创建TopicPartition实例
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            // 5.给Consumer指定消费的Topic和Partition(十分重要)
            // 如果无对应topic或者partition,则会抛出异常IllegalArgumentException
            // 如果此consumer之前已经有过订阅行为且未解除之前所有的订阅,则会抛出异常IllegalStateException
            kafkaConsumer.assign(Collections.singletonList(topicPartition));
            // 6.覆盖原始的Consumer-Topic-Partition对应的Offset,将其设置为指定Offset值
            kafkaConsumer.seek(topicPartition, beginOffset);
            // 7.获取消息
            ConsumerRecords<Object, Object> consumerRecords =
                    kafkaConsumer.poll(Duration.ofMillis(timeInMills));
            // 8.处理消息
            // 将Consumer获取到的消息保存在集合中准备返回
            for (ConsumerRecord record : consumerRecords) {
                // 如果已经读完最多消息限制则直接跳出循环
                if (recordList.size() == maxReads) break;
                // 如果还需要读取则继续读取
                recordList.add(record);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 9.释放资源
            if (kafkaConsumer != null) kafkaConsumer.close();
        }
        return recordList;
    }

    // 根据指定的Brokers/Topic/Partition获取对应的最新的Leader
    // (不做,因为Kafka会自动选举新的Partition-Leader)
    public static void findNewLeader(Map<String, Integer> brokers, String topic, int partition) {

    }

}
