package com.tomandersen.kafka.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

/**
 * <h3>Producer Interceptor测试</h3>
 * 此类主要用于测试自定义实现的Interceptor是否可用.
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/3/19
 * @see ProducerConfig
 */
public class ProducerInterceptorDemo {
    public static void main(String[] args) {
        // 1.设置kafka producer相关配置信息
        // 这些配置信息都是org.apache.kafka.clients.producer.ProducerConfig类中属性,可以查看源码中对应的Javadoc介绍
        // 当然也可以直接调用此类中的类变量代替字符串Key
        Properties props = new Properties();
        // 设置kafka集群服务器主机
        props.put("bootstrap.servers", "hadoop101:9092");
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
        // 设置Producer Interceptors链,Interceptor在List中的顺序即是Interceptor的调用顺序
        ArrayList<String> interceptorChain = new ArrayList<>();
        interceptorChain.add("com.tomandersen.kafka.interceptor.TimestampInterceptor");
        interceptorChain.add("com.tomandersen.kafka.interceptor.CountInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptorChain);
        /*// 设置使用自定义的Partitioner,即根据规则判断发向的Partition,而不是在创建ProducerRecord时手动指定
        props.put("partitioner.class", "com.tomandersen.kafka.partitioner.CustomPartitioner");
        // Kafka自带默认的Partitioner,根据key的字节数组创建32bit hash,并映射到所有Partition中
        props.put("partitioner.class","org.apache.kafka.clients.producer.internals.DefaultPartitioner");*/

        // 2.定义KafkaProducer对象
        KafkaProducer<String, String> kafkaProducer = null;

        // 3.分配相应的资源并启动Producer生产者
        try {
            // 创建Producer实例
            kafkaProducer = new KafkaProducer<>(props);
            // 循环调用KafkaProducer.send(ProducerRecord<K,V>)方法来发送消息
            for (int i = 0; i < 10; i++) {
                // 封装ProducerRecord并设置回调类CallBack,实现其onCompletion方法,这是消息发送结束的回调方法
                // 创建ProducerRecord时需要注意,Producer默认是使用Hash的方式选择Partition
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
            if (kafkaProducer != null) kafkaProducer.close();
        }
    }
}
