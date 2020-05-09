package com.aura.bigdata.spark.java.streaming;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

public class MyKafkaProducer<K, V> extends KafkaProducer<K, V> implements Serializable{
    public MyKafkaProducer(Map<String, Object> configs) {
        super(configs);
    }

    public MyKafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        super(configs, keySerializer, valueSerializer);
    }

    public MyKafkaProducer(Properties properties) {
        super(properties);
    }

    public MyKafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        super(properties, keySerializer, valueSerializer);
    }
}
