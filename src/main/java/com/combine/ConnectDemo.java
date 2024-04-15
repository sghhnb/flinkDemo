package com.combine;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Integer> sensorDS1 = env.fromElements(1, 2, 3, 4, 5);

        DataStreamSource<String> sensorDS2 = env.fromElements("11", "333", "224", "567", "4456");

        ConnectedStreams<Integer, String> connect = sensorDS1.connect(sensorDS2);

        SingleOutputStreamOperator<String> map = connect.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return value.toString();
            }

            @Override
            public String map2(String value) throws Exception {
                return value;
            }
        });

        map.print();

        env.execute();
    }

}
