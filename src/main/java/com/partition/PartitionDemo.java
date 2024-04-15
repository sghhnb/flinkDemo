package com.partition;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartitionDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> socketSource = env.socketTextStream("hadoop01", 7777);

        // flink提供7种分区器，1种自定义

        //随机分区
//        socketSource.shuffle().print();

        //轮询
//        socketSource.rebalance().print();

        //缩放
//        socketSource.rescale().print();

        //广播
//        socketSource.broadcast().print();

        //全局
//        socketSource.global().print();

        //指定key发送,keyby

        //forward,一对一

        //自定义
        socketSource.partitionCustom(
                new MyPartitioner(),
                new KeySelector<String, String>() {
                    @Override
                    public String getKey(String value) throws Exception {
                        return value;
                    }
                }
        ).print();

        env.execute();
    }
}
