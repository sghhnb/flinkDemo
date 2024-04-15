package com.watermark;

import com.bean.WaterSensor;
import com.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowJoinDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Tuple2<String, Integer>> sensorDS = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("a", 2),
                Tuple2.of("b", 3),
                Tuple2.of("c", 4),
                Tuple2.of("d", 5)
        );

        SingleOutputStreamOperator<Tuple2<String, Integer>> waterSensorSingleOutputStreamOperator = sensorDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Tuple2<String, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((value, ts) -> value.f1 * 1000L)
        );


        DataStreamSource<Tuple3<String, Integer, Integer>> sensorDS1 = env.fromElements(
                Tuple3.of("a", 1, 1),
                Tuple3.of("a", 11, 2),
                Tuple3.of("b", 6, 3),
                Tuple3.of("c", 9, 4),
                Tuple3.of("d", 7, 5)
        );

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> waterSensorSingleOutputStreamOperator1 = sensorDS1.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Tuple3<String, Integer, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((value, ts) -> value.f1 * 1000L)
        );


        DataStream<String> apply = waterSensorSingleOutputStreamOperator.join(waterSensorSingleOutputStreamOperator1)
                .where(r1 -> r1.f0)
                .equalTo(r2 -> r2.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(
                        new JoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                            @Override
                            public String join(Tuple2<String, Integer> first, Tuple3<String, Integer, Integer> second) throws Exception {
                                return first + "<----->" + second;
                            }
                        }
                );

        apply.print();

        env.execute();

    }
}
