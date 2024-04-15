package com.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountSocketTextStreamDemo {

    public static void main(String[] args) throws Exception {

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<String> socketDS = env.socketTextStream("hadoop01", 7777);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketDS
                .flatMap(
                        (String value, Collector<Tuple2<String, Integer>> out) -> {
                            String[] ss = value.split(" ");
                            for (String index : ss) {
                                out.collect(Tuple2.of(index, 1));
                            }
                        }
                ).setParallelism(2)
                .returns(Types.TUPLE(Types.STRING, Types.INT))      //显示指定类型
                .keyBy(value -> value.f0)
                .sum(1);

        sum.print();

        env.execute();
    }
}


