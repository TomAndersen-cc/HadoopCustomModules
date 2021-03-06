package com.tomandersen.kafka.interceptor;

import org.apache.kafka.clients.producer.*;

import java.util.Map;

/**
 * <h3>Producer Interceptor实现练习</h3>
 * 主要用于练习Producer Interceptor的实现.本类实现的功能是给消息添加时间戳Timestamp,即在消息被
 * 序列化之前给Record消息的Value前端加上当前系统时间戳.
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/3/19
 */
public class TimestampInterceptor implements ProducerInterceptor<String, String> {
    /**
     * This is called from {@link KafkaProducer#send(ProducerRecord)} and
     * {@link KafkaProducer#send(ProducerRecord, Callback)} methods, before key and value
     * get serialized and partition is assigned (if partition is not specified in ProducerRecord).
     * <p>
     * This method is allowed to modify the record, in which case, the new record will be returned. The implication of modifying
     * key/value is that partition assignment (if not specified in ProducerRecord) will be done based on modified key/value,
     * not key/value from the client. Consequently, key and value transformation done in onSend() needs to be consistent:
     * same key and value should mutate to the same (modified) key and value. Otherwise, log compaction would not work
     * as expected.
     * <p>
     * Similarly, it is up to interceptor implementation to ensure that correct topic/partition is returned in ProducerRecord.
     * Most often, it should be the same topic/partition from 'record'.
     * <p>
     * Any exception thrown by this method will be caught by the caller and logged, but not propagated further.
     * <p>
     * Since the producer may run multiple interceptors, a particular interceptor's onSend() callback will be called in the order
     * specified by {@link ProducerConfig#INTERCEPTOR_CLASSES_CONFIG}. The first interceptor
     * in the list gets the record passed from the client, the following interceptor will be passed the record returned by the
     * previous interceptor, and so on. Since interceptors are allowed to modify records, interceptors may potentially get
     * the record already modified by other interceptors. However, building a pipeline of mutable interceptors that depend on the output
     * of the previous interceptor is discouraged, because of potential side-effects caused by interceptors potentially failing to
     * modify the record and throwing an exception. If one of the interceptors in the list throws an exception from onSend(), the exception
     * is caught, logged, and the next interceptor is called with the record returned by the last successful interceptor in the list,
     * or otherwise the client.
     *
     * @param record the record from client or the record returned by the previous interceptor in the chain of interceptors.
     * @return producer record to send to topic/partition
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord record) {
        // 1.直接获取传入的Record的value附加上时间戳后创建新的ProducerRecord并返回
        return new ProducerRecord<>(record.topic(), record.key().toString(),
                System.currentTimeMillis() + "|" + record.value());
    }

    /**
     * This method is called when the record sent to the server has been acknowledged, or when sending the record fails before
     * it gets sent to the server.
     * <p>
     * This method is generally called just before the user callback is called, and in additional cases when <code>KafkaProducer.send()</code>
     * throws an exception.
     * <p>
     * Any exception thrown by this method will be ignored by the caller.
     * <p>
     * This method will generally execute in the background I/O thread, so the implementation should be reasonably fast.
     * Otherwise, sending of messages from other threads could be delayed.
     *
     * @param metadata  The metadata for the record that was sent (i.e. the partition and offset).
     *                  If an error occurred, metadata will contain only valid topic and maybe
     *                  partition. If partition is not given in ProducerRecord and an error occurs
     *                  before partition gets assigned, then partition will be set to RecordMetadata.NO_PARTITION.
     *                  The metadata may be null if the client passed null record to
     *                  {@link KafkaProducer#send(ProducerRecord)}.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    /**
     * This is called when interceptor is closed
     */
    @Override
    public void close() {

    }

    /**
     * Configure this class with the given key-value pairs
     *
     * @param configs Producer配置信息
     */
    @Override
    public void configure(Map<String, ?> configs) {

    }
}
