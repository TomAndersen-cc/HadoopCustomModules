package com.tomandersen.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * <h3>此类为自定义KafkaPartitioner的练习</h3>
 * <p>
 * 主要是通过实现
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/3/17
 */
public class CustomPartitioner implements Partitioner {

    // 保存配置信息
    private Map configs;

    /**
     * Compute the partition for the given record.
     *
     * @param topic      The topic name
     * @param key        The key to partition on (or null if no key)
     * @param keyBytes   The serialized key to partition on( or null if no key)
     * @param value      The value to partition on or null
     * @param valueBytes The serialized value to partition on or null
     * @param cluster    The current cluster metadata
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 直接使用0号Partition,便于测试
        return 0;
    }

    /**
     * This is called when partitioner is closed.
     */
    @Override
    public void close() {

    }


    /**
     * Configure this class with the given key-value pairs
     * 获取配置信息
     *
     * @param configs {@link Map}
     */
    @Override
    public void configure(Map<String, ?> configs) {

        this.configs = configs;
    }
}
