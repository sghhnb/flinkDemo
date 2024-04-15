package com.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class
WordCountStreamDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stringDataStreamSource = env.readTextFile("input/word.txt");

        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = stringDataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String index : words) {
                    Tuple2<String, Integer> stringIntegerTuple2 = Tuple2.of(index, 1);
                    out.collect(stringIntegerTuple2);
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = tuple2SingleOutputStreamOperator.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = tuple2StringKeyedStream.sum(1);

        sum.print();

        env.execute();
    }
}
