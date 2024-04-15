package com.combine;

import com.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UnionDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Integer> sensorDS1 = env.fromElements(1, 2, 3, 4, 5);

        DataStreamSource<Integer> sensorDS2 = env.fromElements(33,4,5,56,6,7,8);

        DataStreamSource<String> sensorDS3 = env.fromElements("11", "333", "224", "567", "4456");

        DataStream<Integer> union = sensorDS1
                .union(sensorDS2)
                .union(sensorDS3.map(index -> Integer.parseInt(index)));

        union.print();

        env.execute();

    }
}
