package com.jd.jr.recommender.mq.functions;

import com.jd.jr.recommender.mq.bean.RecChartData;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Auther: qiuyujiang
 * @Date: 2019/5/9.
 * @Description: 请填写
 */
public class RuleWindowResultFunction implements WindowFunction<Long, RecChartData, Tuple, TimeWindow> {
    /**
     * pv：曝光   pcv：点击
     */
    private String target=null;
    public RuleWindowResultFunction(String target){
        this.target=target;
    }
    @Override
    public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<RecChartData> collector) throws Exception {
        String key = ((Tuple1<String>) tuple).f0;
        Double count = Double.valueOf(input.iterator().next());
        String recloc=key.split("_")[0];
        Integer experimentId=Integer.parseInt(key.split("_")[1]);
        String rule=key.split("_")[2];
        String dt= new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date(window.getEnd()));

        // 统计推荐位实时数据时，只传推荐位编号，实验ID和规则集编码都写成-1
        RecChartData recChartData = new RecChartData(
                RecChartData.TYPE_REALTIME,
                recloc,
                experimentId,
                rule,
               target,
                Double.valueOf(String.valueOf(count)),
                dt
        );
        collector.collect(recChartData);
    }
}
