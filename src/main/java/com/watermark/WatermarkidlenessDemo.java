package com.watermark;

import com.bean.WaterSensor;
import com.function.WaterSensorMapFunction;
import com.partition.MyPartitioner;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WatermarkidlenessDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        //自定义分区器，数据%分区数，只输入奇数，都只会去往map的一个子任务
        SingleOutputStreamOperator<Integer> socketDS = env
                .socketTextStream("hadoop01", 7777)
                .partitionCustom(
                        new MyPartitioner(),
                        r -> r
                )
                .map(r -> Integer.parseInt(r))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Integer>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Integer>() {
                                            @Override
                                            public long extractTimestamp(Integer element, long recordTimestamp) {
                                                return element * 1000L;
                                            }
                                        }
                                )
                                .withIdleness(Duration.ofSeconds(5))
                );

        //分成两组
        SingleOutputStreamOperator<String> process = socketDS.keyBy(
                        new KeySelector<Integer, Integer>() {
                            @Override
                            public Integer getKey(Integer value) throws Exception {
                                return value % 2;
                            }
                        }
                ).window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .process(
                        new ProcessWindowFunction<Integer, String, Integer, TimeWindow>() {
                            @Override
                            public void process(Integer integer, ProcessWindowFunction<Integer, String, Integer, TimeWindow>.Context context, Iterable<Integer> elements, Collector<String> out) throws Exception {
                                long startTimeLong = context.window().getStart();
                                long endTimeLong = context.window().getEnd();
                                String startTime = DateFormatUtils.format(startTimeLong, "yyyy-MM-dd HH:mm:ss.SSS");
                                String endTime = DateFormatUtils.format(endTimeLong, "yyyy-MM-dd HH:mm:ss.SSS");
                                long count = elements.spliterator().estimateSize();
                                out.collect("key=" + integer + "的窗口[" + startTime + "," + endTime + ")包含" + count + "条数据");
                            }
                        }
                );

        process.print();

        env.execute();

    }
}
