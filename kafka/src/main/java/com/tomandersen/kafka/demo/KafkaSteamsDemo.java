package com.tomandersen.kafka.demo;

import com.tomandersen.kafka.processor.LogProcessor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import java.util.Properties;

/**
 * <p>本类主要用于演示{@link KafkaStreams}的使用方法,同时也是练习.
 * KafkaStreams所需配置信息可查看{@link StreamsConfig}.
 * (已测试)
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/3/19
 */
public class KafkaSteamsDemo {
    public static void main(String[] args) {

        // 1.配置相关参数
        Properties props = new Properties();
        // 设置Application ID
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "KafkaStreamsDemo");
        // 设置Brokers集群
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop101:9092,hadoop102:9092,hadoop103:9092");
        // 设置Key和Value的序列化/反序列化器
        /*props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());*/
        // 定义Stream的数据源和数据池Topic
        String streamSourceTopic = "first";
        String streamSinkTopic = "second";

        // 2.创建StreamsBuilder实例
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // 3.创建并配置Streams拓扑结构
        Topology topology = streamsBuilder.build()
                .addSource("StreamSource", streamSourceTopic)
                .addProcessor("StreamProcessor", new ProcessorSupplier<byte[], byte[]>() {
                    @Override
                    public Processor<byte[], byte[]> get() {
                        return new LogProcessor();
                    }
                }, "StreamSource")
                .addSink("StreamSink", streamSinkTopic, "StreamProcessor");

        // 4.创建KafkaStreams实例
        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);

        // 5.启动KafkaStreams
        kafkaStreams.start();
    }
}
