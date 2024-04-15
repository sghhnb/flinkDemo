package com.split;

import com.partition.MyPartitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SplitByFilterDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> socketSource = env.socketTextStream("hadoop01", 7777);

        //使用分流,filter实现
        socketSource.filter(value -> Integer.parseInt(value) % 2 == 0).print("偶数流");
        socketSource.filter(value -> Integer.parseInt(value) % 2 == 1).print("奇数流");



        env.execute();
    }
}
