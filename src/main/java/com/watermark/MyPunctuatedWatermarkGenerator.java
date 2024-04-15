package com.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class MyPunctuatedWatermarkGenerator<T> implements WatermarkGenerator<T> {

    private long delayTs;
    private long maxTs;

    public MyPunctuatedWatermarkGenerator(long delayTs) {
        this.delayTs = delayTs;
        this.maxTs = Long.MIN_VALUE + this.delayTs + 1;
    }

    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        maxTs = Math.max(maxTs, eventTimestamp);
        output.emitWatermark(new Watermark(maxTs - delayTs - 1));
        System.out.println("调用了onEvent方法，获取目前最大的时间戳："+maxTs);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
    }
}
