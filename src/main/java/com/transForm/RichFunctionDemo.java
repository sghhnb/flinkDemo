package com.transForm;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichFunctionDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4);

        SingleOutputStreamOperator<Integer> map = source.map(new RichMapFunction<Integer, Integer>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                RuntimeContext runtimeContext = getRuntimeContext();    //获取上下文信息
                int indexOfThisSubtask = runtimeContext.getIndexOfThisSubtask();
                String taskNameWithSubtasks = runtimeContext.getTaskNameWithSubtasks();
                System.out.println("子任务编号：" + indexOfThisSubtask + "启动，子任务名称：" + taskNameWithSubtasks + "调用open");
            }

            @Override
            public void close() throws Exception {
                super.close();
                RuntimeContext runtimeContext = getRuntimeContext();    //获取上下文信息
                int indexOfThisSubtask = runtimeContext.getIndexOfThisSubtask();
                String taskNameWithSubtasks = runtimeContext.getTaskNameWithSubtasks();
                System.out.println("子任务编号：" + indexOfThisSubtask + "启动，子任务名称：" + taskNameWithSubtasks + "调用open");
            }

            @Override
            public Integer map(Integer value) throws Exception {
                return value + 1;
            }
        });

        map.print();

        env.execute();
    }
}
