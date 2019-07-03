package com.jd.jr.recommender.mq.functions;

import com.jd.jr.recommender.mq.bean.StandardizedMq;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @Auther: qiuyujiang
 * @Date: 2019/5/9.
 * @Description: 请填写
 */
public class RecPvAggregation implements AggregateFunction<Tuple2<String, StandardizedMq>, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Tuple2<String, StandardizedMq> tuple, Long accumulator) {
        long result = accumulator + 1;
        return result;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long accumulator1, Long accumulator2) {
        return accumulator1 + accumulator2;
    }
}
