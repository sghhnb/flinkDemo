package com.watermark;

import com.bean.WaterSensor;
import com.function.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class WatermarkAllowLatenessDemo {

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

        OutputTag<WaterSensor> waterSensorOutputTag = new OutputTag<>("late-data", Types.POJO(WaterSensor.class));

        SingleOutputStreamOperator<String> process = waterSensorSingleOutputStreamOperator.keyBy(
                        new KeySelector<WaterSensor, String>() {
                            @Override
                            public String getKey(WaterSensor value) throws Exception {
                                return value.getId();
                            }
                        }
                )
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(2))       //推迟两秒关窗
                .sideOutputLateData(waterSensorOutputTag)   //关窗后迟到数据，放入侧输出流
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

        process.getSideOutput(waterSensorOutputTag).printToErr("关窗后的数据");
        env.execute();

    }
}
