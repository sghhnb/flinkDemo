package com.process;

import com.bean.WaterSensor;
import com.function.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class KeyedProcesstTimerDemo {

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

        KeyedStream<WaterSensor, String> waterSensorStringKeyedStream = waterSensorSingleOutputStreamOperator.keyBy(
                new KeySelector<WaterSensor, String>() {
                    @Override
                    public String getKey(WaterSensor value) throws Exception {
                        return value.getId();
                    }
                }
        );
        SingleOutputStreamOperator<String> process = waterSensorStringKeyedStream.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        Long timestamp = ctx.timestamp();

                        //定时器
                        TimerService timerService = ctx.timerService();
                        //注册事件时间定时器
                        timerService.registerEventTimeTimer(5000L);

                        //获取当前watermark是上一次的watermark
                        long l = timerService.currentWatermark();

                        //注册处理时间定时器
//                        timerService.registerProcessingTimeTimer();
//                        timerService.deleteEventTimeTimer();
//                        timerService.deleteProcessingTimeTimer();
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        System.out.println("现在时间是：" + timestamp+"定时器");
                    }
                }
        );

        process.print();

        env.execute();

    }
}
