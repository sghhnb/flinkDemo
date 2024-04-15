package com.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class CollectionDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从集合读取数据
        DataStreamSource<Integer> source =
                env.fromElements(1, 3, 4, 3, 1);
//                env.fromCollection(Arrays.asList(1, 3, 4, 3, 1));

        source.print();

        env.execute();
    }
}
