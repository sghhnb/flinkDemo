package com.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCountBatchDemo {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 读取文件
        DataSource<String> lineDs = env.readTextFile("input/word.txt");
        // flaMap拍扁
        FlatMapOperator<String, Tuple2<String, Integer>> stringTuple2FlatMapOperator = lineDs.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] strs = value.split(" ");
                for (String index : strs) {
                    Tuple2<String, Integer> stringIntegerTuple2 = Tuple2.of(index, 1);
                    out.collect(stringIntegerTuple2);
                }
            }
        });
        //group by 分组
        UnsortedGrouping<Tuple2<String, Integer>> tuple2UnsortedGrouping = stringTuple2FlatMapOperator.groupBy(0);
        //分组统计个数
        AggregateOperator<Tuple2<String, Integer>> sum = tuple2UnsortedGrouping.sum(1);
        //打印
        sum.print();


    }
}
