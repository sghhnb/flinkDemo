package com.sink;

import com.bean.WaterSensor;
import com.function.WaterSensorMapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SinkMySql {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //必须开启checkpoint，否则精准一次，无法写入kafka
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop01", 7777)
                .map(new WaterSensorMapFunction());

        SinkFunction<WaterSensor> mySqlSink = JdbcSink.sink(
                "insert into ws values(?,?,?)",
                new JdbcStatementBuilder<WaterSensor>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, WaterSensor waterSensor) throws SQLException {
                        preparedStatement.setString(1, waterSensor.getId());
                        preparedStatement.setLong(2, waterSensor.getTs());
                        preparedStatement.setInt(3, waterSensor.getVc());
                    }
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3)
                        .withBatchSize(100)
                        .withBatchIntervalMs(3000)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("")
                        .withPassword("admin")
                        .withUsername("admin")
                        .withConnectionCheckTimeoutSeconds(60)
                        .build()
        );

        sensorDS.addSink(mySqlSink);

        env.execute();

    }
}
