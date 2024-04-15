package com.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class KafkaSourceDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop01:9092,hadoop02:9092,hadoop03:9092")
                .setGroupId("test")
                .setTopics("test1")
                .setValueOnlyDeserializer(new SimpleStringSchema()) //指定反序列化器，只反序列化value
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        env.fromSource(kafkaSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3)), "kafkaSource")
                .print();

        env.execute();

    }
}
