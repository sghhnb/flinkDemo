package com.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class SinkKafkaWithKey {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //必须开启checkpoint，否则精准一次，无法写入kafka
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        DataStreamSource<String> sensorDS = env.socketTextStream("hadoop01", 7777);

        //指定写入kafka的key, 可以自定义序列化器
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("hadoop01:9092,hadoop02:9092,hadoop03:9092")
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<String>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(String element, KafkaSinkContext context, Long timestamp) {
                                String[] datas = element.split(",");
                                byte[] key = datas[0].getBytes(StandardCharsets.UTF_8);
                                byte[] value = element.getBytes(StandardCharsets.UTF_8);
                                return new ProducerRecord<>("ws", key, value);
                            }
                        }
                )
                //保证数据发送不丢不重,精准一次，至少一次
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //事务id前缀,如果精准一次，必须设置
                .setTransactionalIdPrefix("outputKafka")
                //如果精准一次，必须设置事务超时时间，大于checkpoint间隔，小于max 15分钟
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10 * 60 * 1000 + "")
                .build();

        sensorDS.sinkTo(kafkaSink);

        env.execute();

    }
}
