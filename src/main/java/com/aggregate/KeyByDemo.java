package com.aggregate;

import com.bean.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 234L, 211),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s2", 24L, 21),
                new WaterSensor("s3", 3L, 3)
        );

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });

        sensorKS.print();

        env.execute();


    }
}
