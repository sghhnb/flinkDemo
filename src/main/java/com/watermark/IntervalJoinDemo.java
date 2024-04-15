package com.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class IntervalJoinDemo {

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


        OutputTag<Tuple2<String, Integer>> leftLate = new OutputTag<>("leftLate", Types.TUPLE(Types.STRING, Types.INT));
        OutputTag<Tuple3<String, Integer, Integer>> rightLate = new OutputTag<>("rightLate", Types.TUPLE(Types.STRING, Types.INT, Types.INT));

        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = waterSensorSingleOutputStreamOperator.keyBy(r -> r.f0);
        KeyedStream<Tuple3<String, Integer, Integer>, String> tuple3StringKeyedStream = waterSensorSingleOutputStreamOperator1.keyBy(r -> r.f0);
        SingleOutputStreamOperator<String> process = tuple2StringKeyedStream
                .intervalJoin(tuple3StringKeyedStream)
                .between(Time.seconds(-2), Time.seconds(2))
                .sideOutputLeftLateData(leftLate)
                .sideOutputRightLateData(rightLate)
                .process(
                        new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                            @Override
                            public void processElement(Tuple2<String, Integer> left, Tuple3<String, Integer, Integer> right, ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                                out.collect(left + "<----->" + right);
                            }
                        }
                );


        process.print();
        process.getSideOutput(leftLate).printToErr("左流");
        process.getSideOutput(rightLate).printToErr("右流");

        env.execute();

    }
}
