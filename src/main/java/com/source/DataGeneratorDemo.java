package com.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataGeneratorDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /**
         * 数据生成器Source, 四个参数
         * 第一个：GeneratorFunction接口，需要实现，重写map方法，输入类型固定为Long
         * 第二个：long类型，自动生成的数据序列（从0自增）的最大值，达到这个值就停止
         * 第三个：限速策略，比如每秒生成几条数据
         * 第四个：返回的类型
         */
        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long aLong) throws Exception {
                return "number:" + aLong;
            }
        },
                10,
                RateLimiterStrategy.perSecond(1),
                Types.STRING);

        env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "dataGeneratorSource")
                .print();

        env.execute();


    }
}
