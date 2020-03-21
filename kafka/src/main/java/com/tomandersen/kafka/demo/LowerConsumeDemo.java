package com.tomandersen.kafka.demo;

import com.tomandersen.kafka.util.CustomConsumes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 此类主要是用于通过{@link KafkaConsumer}练习Consumer的底层操作.
 * 实际上就是封装原始API,进行些许定制化，无特别意义.
 *
 * <p>
 * 主要练习的功能有:
 * 集群中查找指定Partition的Leader/查找Consumer指定Partition的Offset/
 * 根据Offset消费最新的消息/在Replication中寻找Leader.
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/3/14
 * @see KafkaConsumer
 */
public class LowerConsumeDemo {

//    // 保存指定Topic-Partition的所有副本
//    private Node[] replications;
//
//
//    // 根据指定的Brokers/Topic/Partition返回对应的Leader
//    public Node findLeader(Map<String, Integer> brokers, String topic, int partition) {
//
//        // 1.通过接收到的配置信息创建KafkaConsumer实例
//        Properties props = new Properties();
//        for (String broker : brokers.keySet()) {
//            props.put("bootstrap.servers",
//                    broker + ":" + brokers.getOrDefault(broker, 9092));
//        }
//        // 设置键值对<K,V>反序列化器,因为都是String类型,所以使用String对象反序列化器
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//
//        // 2.定义KafkaConsumer实例,采用try{}catch{}finally{}的方式获取资源
//        KafkaConsumer<String, String> kafkaConsumer = null;
//        try {
//            // 创建KafkaConsumer实例
//            kafkaConsumer = new KafkaConsumer<>(props);
//            // 通过KafkaConsumer实例获取指定Topic对应的Partition信息
//            List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topic);
//            // 遍历返回的指定Topic的所有Partition信息
//            for (PartitionInfo partitionInfo : partitionInfos) {
//                // 若当前Partition是指定的Partition,则保存此Partition的Replication并将其Leader返回
//                if (partitionInfo.partition() == partition) {
//                    // 获取指定Partition的所有副本
//                    replications = partitionInfo.replicas();
//                    return partitionInfo.leader();
//                }
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            if (kafkaConsumer != null) kafkaConsumer.close();
//        }
//        // 如果没有找到Leader则返回null
//        return null;
//    }
//
//
//    // 根据指定的Brokers/Topic/Partition获取对应的lastOffset
//    public long getLastOffset(Map<String, Integer> brokers, String topic, int partitionID) {
//        // 1.通过接收到的配置信息创建KafkaConsumer实例
//        Properties props = new Properties();
//        for (String broker : brokers.keySet()) {
//            props.put("bootstrap.servers",
//                    broker + ":" + brokers.getOrDefault(broker, 9092));
//        }
//        // 设置键值对<K,V>反序列化器,因为都是String类型,所以使用String对象反序列化器
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//
//        // 2.创建Consumer实例
//        KafkaConsumer<Object, Object> kafkaConsumer = new KafkaConsumer<>(props);
//        // 3.调用已实现的同名方法
//        return getLastOffset(kafkaConsumer, topic, partitionID);
//    }
//
//    // 根据指定的Consumer/Topic/Partition获取对应的lastOffset,因为同一个Consumer可能有多个Topic
//    public long getLastOffset(Consumer consumer, String topic, int partitionID) {
//        // 1.创建TopicPartition实例
//        TopicPartition topicPartition = new TopicPartition(topic, partitionID);
//        // 2.订阅Topic,否则会抛出AuthorizationException异常
//        consumer.subscribe(Collections.singletonList(topic));
//        // 3.获取对应Topic的最后Offset
//        Map offsets = consumer.endOffsets(
//                Collections.singletonList(topicPartition));
//        // 4.返回对应Partition的Offset,如果不存在则直接返回0
//        return (Long) offsets.getOrDefault(topicPartition, 0L);
//    }
//
//    // 根据指定的Brokers/Topic/Partition/Offset/timeInMills/maxReads在指定时间内读取限定数量的消息
//    public void consumeFrom(Map<String, Integer> brokers, String topic, int partitionID, long offset, long timeInMills, long maxReads) {
//        // 1.通过接收到的配置信息创建KafkaConsumer实例
//        Properties props = new Properties();
//        for (String broker : brokers.keySet()) {
//            props.put("bootstrap.servers",
//                    broker + ":" + brokers.getOrDefault(broker, 9092));
//        }
//        // 设置键值对<K,V>反序列化器,因为都是String类型,所以使用String对象反序列化器
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//
//        // 2.定义KafkaConsumer对象
//        KafkaConsumer<Object, Object> kafkaConsumer = null;
//        try {
//            // 3.创建KafkaConsumer实例
//            kafkaConsumer = new KafkaConsumer<>(props);
//            // 4.创建TopicPartition实例
//            TopicPartition topicPartition = new TopicPartition(topic, partitionID);
//            // 5.给Consumer指定消费的Topic和Partition(十分重要)
//            // 如果无对应topic或者partition,则会抛出异常IllegalArgumentException
//            // 如果此consumer之前已经有过订阅行为且未解除之前所有的订阅,则会抛出异常IllegalStateException
//            kafkaConsumer.assign(Collections.singletonList(topicPartition));
//            // 6.覆盖原始的Topic/Partition对应的Offset,将其设置为指定Offset值
//            kafkaConsumer.seek(topicPartition, offset);
//            // 7.消费消息
//            // 然后进行正常正常消费,可以使用自定义的ConsumerRunner,也可以直接打印
//            // 这里测试需要选择直接打印,且只进行单次遍历
//            ConsumerRecords<Object, Object> consumerRecords =
//                    kafkaConsumer.poll(Duration.ofMillis(timeInMills));
//            // 8.处理消息
//            // 直接打印Consumer在15000ms内获取到的消息
//            for (ConsumerRecord record : consumerRecords) {
//                // 如果已经读完最多消息限制则直接跳出循环
//                if (maxReads-- == 0) break;
//                // 如果还需要读取则继续读取
//                System.out.println(record);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            // 9.释放资源
//            if (kafkaConsumer != null) kafkaConsumer.close();
//        }
//    }
//
//    // 根据指定的Brokers/Topic/Partition/Offset/timeInMills消费一定时间的消息
//    public void consumeFrom(Consumer consumer, String topic, int partitionID, long offset, long timeInMills) {
//        // 1.指定消费的topic和partition
//        TopicPartition topicPartition = new TopicPartition(topic, partitionID);
//        // 2.设置consumer读取指定的topic和partition
//        // 如果无对应topic或者partition,则会抛出异常IllegalArgumentException
//        // 如果此consumer之前已经有过订阅行为且未解除之前所有的订阅,则会抛出异常IllegalStateException
//        consumer.assign(Collections.singletonList(topicPartition));
//        // 3.设置读取指定的topic-partition-offset
//        consumer.seek(topicPartition, offset);
//        // 4.使用设置好的consumer进行消费,并指定消费时长
//        ConsumerRecords consumerRecords =
//                consumer.poll(Duration.ofMillis(timeInMills));
//        // 5.处理获取到的消息,此处为了便于测试选择直接打印
//        System.out.println(consumerRecords);
//    }
//
//    // 根据指定的Brokers/Topic/Partition获取对应的最新的Leader(不做)
//    public void findNewLeader(Map<String, Integer> brokers, String topic, int partition) {
//
//    }

    public static void main(String[] args) {


        // 1.配置集群信息
        Map<String, Integer> kafkaBrokers = new HashMap<>();
        kafkaBrokers.put("hadoop101", 9092);
        kafkaBrokers.put("hadoop102", 9092);
        kafkaBrokers.put("hadoop103", 9092);

        // 2.设置Topic/Partition/Offset/maxReads
        String topic = "first";
        int partition = 0;
        // 指定的Offset,Offset的值指的是消息序号
        long offset = Long.parseLong("2");
        // 最大读取消息数量
        long maxReads = Long.parseLong("10");

        // 3.通过指定的Brokers/Topic/Partition获取Leader(测试成功)
        Node leader = CustomConsumes.findLeader(kafkaBrokers, topic, partition);
        System.out.println("The leaderID of " + topic + "-" + partition + " is " +
                leader.id() + ":" + leader.host());
        // 4.通过指定的Brokers/Topic/Partition/Offset/TimeInMills/MaxReads消费消息(测试成功)
        List<ConsumerRecord> records = CustomConsumes.getRecordsFrom(kafkaBrokers, topic, partition, offset, 15000, maxReads);
        System.out.println(records);
        // 5.通过指定的Consumer/Topic/Partition获取对应最后Offset(测试成功)
        long lastOffset = CustomConsumes.getLastOffset(kafkaBrokers, topic, partition);
        System.out.println("The last offset of " + topic + "-" + partition
                + " is " + lastOffset);

    }
}
