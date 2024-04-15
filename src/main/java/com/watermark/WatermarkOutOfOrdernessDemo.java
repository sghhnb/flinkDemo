package com.watermark;

import com.bean.WaterSensor;
import com.function.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.time.Duration;

public class WatermarkOutOfOrdernessDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop01", 7777).map(new WaterSensorMapFunction());

        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(
                        new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                System.out.println("数据=" + element + ",recordTs=" + recordTimestamp);
                                return element.getTs() + 1000L;
                            }
                        }
                );

        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = sensorDS.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);

        SingleOutputStreamOperator<String> process = waterSensorSingleOutputStreamOperator.keyBy(
                        new KeySelector<WaterSensor, String>() {
                            @Override
                            public String getKey(WaterSensor value) throws Exception {
                                return value.getId();
                            }
                        }
                )
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(
                        new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                                long startTimeLong = context.window().getStart();
                                long endTimeLong = context.window().getEnd();
                                String startTime = DateFormatUtils.format(startTimeLong, "yyyy-MM-dd HH:mm:ss.SSS");
                                String endTime = DateFormatUtils.format(endTimeLong, "yyyy-MM-dd HH:mm:ss.SSS");
                                long count = elements.spliterator().estimateSize();
                                out.collect("key=" + s + "的窗口[" + startTime + "," + endTime + ")包含" + count + "条数据");
                            }
                        }
                );

        process.print();

        env.execute();

    }
}
