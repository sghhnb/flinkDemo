package com.function;

import com.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

public class MyMapFunction implements MapFunction<WaterSensor, String> {


    @Override
    public String map(WaterSensor value) throws Exception {
        return value.getId();
    }
}
