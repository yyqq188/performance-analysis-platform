package com.pactera.yhl.sink.abstr;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public abstract class AbstractCKSink<T> extends RichSinkFunction<T> {
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        super.invoke(value, context);
    }

    public abstract void handler(T value, Context context) throws Exception;
}
