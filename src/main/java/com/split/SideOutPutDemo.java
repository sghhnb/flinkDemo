package com.split;

import com.bean.WaterSensor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutPutDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );
        OutputTag<WaterSensor> s1SplitStream = new OutputTag<>("s1SplitStream", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> s2SplitStream = new OutputTag<>("s2SplitStream", Types.POJO(WaterSensor.class));
        //使用分流
        SingleOutputStreamOperator<WaterSensor> process = sensorDS.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, ProcessFunction<WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                String id = value.getId();
                if ("s1".equals(id)) {
                    ctx.output(s1SplitStream, value);
                } else if ("s2".equals(id)) {
                    ctx.output(s2SplitStream, value);
                } else {
                    out.collect(value);
                }
            }
        });

        //主流
        process.print();

        //侧流
        process.getSideOutput(s1SplitStream).print();
        process.getSideOutput(s2SplitStream).print();

        env.execute();
    }
}
