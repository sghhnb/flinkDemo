package com.window;

import com.bean.WaterSensor;
import com.function.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowApiDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //必须开启checkpoint，否则精准一次，无法写入kafka
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop01", 7777)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });

        //窗口内所有数据进入同一个流
//        sensorDS.windowAll()

        //基于时间的滚动窗口
        SingleOutputStreamOperator<WaterSensor> reduce = sensorKS
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce(
                        new ReduceFunction<WaterSensor>() {
                            @Override
                            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                                return new WaterSensor(value1.getId(), value2.getTs(), value1.getVc() + value2.getVc());
                            }
                        }
                );
        reduce.print();

        SingleOutputStreamOperator<String> aggregate = sensorKS
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(
                        new AggregateFunction<WaterSensor, Integer, String>() {
                            @Override
                            public Integer createAccumulator() {
                                System.out.println("创建累加器");
                                return 0;
                            }

                            @Override
                            public Integer add(WaterSensor value, Integer accumulator) {
                                System.out.println("调用add方法");
                                return accumulator + value.getVc();
                            }

                            @Override
                            public String getResult(Integer accumulator) {
                                System.out.println("调用get方法");
                                return accumulator.toString();
                            }

                            @Override
                            public Integer merge(Integer a, Integer b) {
                                System.out.println("调用merge方法");
                                return null;
                            }
                        }
                );
        aggregate.print();


//        sensorKS.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2)));

//        sensorKS.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)));

        //基于计数
//        sensorKS.countWindow(5);
//        sensorKS.countWindow(5, 2);
//        sensorKS.window(GlobalWindows.create());//需要自定义触发器


        //全窗口模式




        env.execute();

    }
}
