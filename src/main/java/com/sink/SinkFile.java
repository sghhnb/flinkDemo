package com.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;

public class SinkFile {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        //必须开启checkpoint，否则肯定是inprogress
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long aLong) throws Exception {
                return "number:" + aLong;
            }
        },
                100,
                RateLimiterStrategy.perSecond(1),
                Types.STRING);

        DataStreamSource<String> dataGen = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "dataGeneratorSource");

        FileSink<String> fileSinkOutPut = FileSink
                .<String>forRowFormat(
                        new Path("output"),
                        new SimpleStringEncoder<>("UTF-8")
                )
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("example-")
                                .withPartSuffix(".log")
                                .build()
                )
                //分桶
                .withBucketAssigner(
                        new DateTimeBucketAssigner<>("yyyy-MM-dd HH", ZoneId.systemDefault())
                )
                //文件滚动策略 10s, 1M
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofSeconds(10))
                                .withMaxPartSize(new MemorySize(1024 * 1024))
                                .build()
                )
                .build();


        dataGen.sinkTo(fileSinkOutPut);

        env.execute();

    }
}
