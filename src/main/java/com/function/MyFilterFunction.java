package com.function;

import com.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;

public class MyFilterFunction implements FilterFunction<WaterSensor> {

    public String id;

    public MyFilterFunction(String id) {
        this.id = id;
    }

    @Override
    public boolean filter(WaterSensor value) throws Exception {
        return this.id.equals(value.getId());
    }
}
