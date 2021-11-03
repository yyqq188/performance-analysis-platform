package com.pactera.yhl.sink.abstr;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public abstract class AbstractRedisSink<IN> extends RichSinkFunction<IN> {
}
