package com.jd.jr.recommender.mq;

import com.alibaba.fastjson.JSON;
import com.jd.jr.recommender.mq.bean.JDBCProperties;
import com.jd.jr.recommender.mq.bean.JmqProperties;
import com.jd.jr.recommender.mq.bean.RecChartData;
import com.jd.jr.recommender.mq.bean.StandardizedMq;
import com.jd.jr.recommender.mq.functions.*;
import com.jd.jr.recommender.mq.sink.JDBCSink;
import com.jd.jr.recommender.mq.source.FakeSource;
import com.jd.jr.recommender.mq.source.JmqSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
public class RecCalculator {

    private final String PV_TARGET="pv";
    private final String PCV_TARGET="pcv";

    public static void main(String[] args) throws Exception {
        JmqProperties jmqProperties = JmqProperties.loadProperties();
        JDBCProperties jdbcProperties = JDBCProperties.loadProperties();
        RecCalculator recCalculator =new RecCalculator();
        List<String> reclocList=new ArrayList<String>();
        reclocList.add("100045");
        reclocList.add("100057");
        reclocList.add("100058");
        final List<String> reclocs=reclocList;

        // 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Boolean localDev = Boolean.valueOf(PropertyUtil.getInstance().getProperty("flink.localDev"));
        if (localDev) {
            env.setParallelism(1);
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        } else {
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        }

        DataStreamSource<String> standardizedMq = null;
        if (localDev) {
            standardizedMq = env.addSource(new FakeSource());
        } else {
            standardizedMq = env.addSource(new JmqSource(jmqProperties));
        }

        //全部数据
         SingleOutputStreamOperator<StandardizedMq> singleOutputStreamOperator= standardizedMq
                .map(new MapFunction<String, StandardizedMq>() {
                    @Override
                    public StandardizedMq map(String json) throws Exception {
                        return JSON.parseObject(json, StandardizedMq.class);
                    }
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<StandardizedMq>() {
                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        long currentWatermark = System.currentTimeMillis();
                        return new Watermark(currentWatermark);
                    }

                    @Override
                    public long extractTimestamp(StandardizedMq element, long previousElementTimestamp) {
                        long timestamp = System.currentTimeMillis();
                        try {
                            timestamp = Long.parseLong( element.getTimestamp());
                        } catch (NumberFormatException e){
                            e.printStackTrace();
                        }
                        return timestamp;
                    }
                });
//        *******************************按照统计指标过滤数据***************************************
        //分支曝光
        SingleOutputStreamOperator<StandardizedMq> singleOutputStreamOperatorBranchPv=singleOutputStreamOperator
                .filter(new FilterFunction<StandardizedMq>(){
                    @Override
                    public boolean filter(StandardizedMq standardizedMq) throws Exception {
                        final String PV="imp";
                        //只保留曝光
                        return PV.equals(standardizedMq.getType())&&reclocs.contains(standardizedMq.getRecloc());
                    }
                });
        //分支点击
        SingleOutputStreamOperator<StandardizedMq> singleOutputStreamOperatorBranchPcv=singleOutputStreamOperator
                .filter(new FilterFunction<StandardizedMq>(){
                    @Override
                    public boolean filter(StandardizedMq standardizedMq) throws Exception {
                        final String PV="clck";
                        //只保留曝光
                        return PV.equals(standardizedMq.getType())&&reclocs.contains(standardizedMq.getRecloc());
                    }
                });
        //数据部曝光
        SingleOutputStreamOperator<StandardizedMq> singleOutputStreamOperatorPv= singleOutputStreamOperator
                .filter(new FilterFunction<StandardizedMq>(){
                    @Override
                    public boolean filter(StandardizedMq standardizedMq) throws Exception {
                        final String PV="imp";
                        final String TYPE="1";
                        //只保留曝光且为数据部流量
                        return PV.equals(standardizedMq.getType())&&TYPE.equals(standardizedMq.getBranch())&&reclocs.contains(standardizedMq.getRecloc());
                    }
                });
        //数据部点击
        SingleOutputStreamOperator<StandardizedMq> singleOutputStreamOperatorPcv= singleOutputStreamOperator
                .filter(new FilterFunction<StandardizedMq>(){
                    @Override
                    public boolean filter(StandardizedMq standardizedMq) throws Exception {
                        final String PV="clck";
                        final String TYPE="1";
                        //只保留曝光且为数据部流量
                        return PV.equals(standardizedMq.getType())&&TYPE.equals(standardizedMq.getBranch())&&reclocs.contains(standardizedMq.getRecloc());
                    }
                });

// **************统计实时pv***********************
        //分流
        DataStream<RecChartData> branchPv=recCalculator.branchCount(singleOutputStreamOperatorBranchPv,recCalculator.PV_TARGET);
        recCalculator.execute(branchPv,localDev,jdbcProperties);
        //推荐位
        DataStream<RecChartData> reclocPv=recCalculator.recCount(singleOutputStreamOperatorPv,recCalculator.PV_TARGET);
        recCalculator.execute(reclocPv,localDev,jdbcProperties);
        //试验位
        DataStream<RecChartData> expPv=recCalculator.expCount(singleOutputStreamOperatorPv,recCalculator.PV_TARGET);
        recCalculator.execute(expPv,localDev,jdbcProperties);
        //规则集
        DataStream<RecChartData> rulePv=recCalculator.ruleCount(singleOutputStreamOperatorPv,recCalculator.PV_TARGET);
        recCalculator.execute(rulePv,localDev,jdbcProperties);

//        *********************统计实时pcv********************
        DataStream<RecChartData> branchPcv=recCalculator.branchCount(singleOutputStreamOperatorBranchPcv,recCalculator.PCV_TARGET);
        recCalculator.execute(branchPcv,localDev,jdbcProperties);
        //推荐位
        DataStream<RecChartData> reclocPcv=recCalculator.recCount(singleOutputStreamOperatorPcv,recCalculator.PCV_TARGET);
        recCalculator.execute(reclocPcv,localDev,jdbcProperties);
        //试验位
        DataStream<RecChartData> expPcv=recCalculator.expCount(singleOutputStreamOperatorPcv,recCalculator.PCV_TARGET);
        recCalculator.execute(expPcv,localDev,jdbcProperties);
        //规则集
        DataStream<RecChartData> rulePcv=recCalculator.ruleCount(singleOutputStreamOperatorPcv,recCalculator.PCV_TARGET);
        recCalculator.execute(rulePcv,localDev,jdbcProperties);

//        *******************实时点击率**********************
//        double branchPvValue=branchPv.countWindowAll();

        System.out.println("start...");
        env.execute("rec_yxmx_window_count");
    }



    /**
     * 处理统计结果
     */
    public void execute(DataStream<RecChartData> dataDataStream,boolean localDev,JDBCProperties jdbcProperties){
        if (localDev) {
            dataDataStream.print();
        } else {
            dataDataStream.addSink(new JDBCSink(jdbcProperties));
        }
    }

    /**
     * 统计分支流量
     * @param singleOutputStreamOperator
     */
    public  DataStream<RecChartData> branchCount(SingleOutputStreamOperator<StandardizedMq> singleOutputStreamOperator, String target){
        //统计分支流量实时pv
             return singleOutputStreamOperator
                .map(new MapFunction<StandardizedMq, Tuple2<String, StandardizedMq>>() {
                    @Override
                    public Tuple2<String, StandardizedMq> map(StandardizedMq standardizedMq) throws Exception {
                        // 统计推荐位实时数据时，只传推荐位编号，实验ID和规则集编码都写成-1
                        return new Tuple2<String, StandardizedMq>(standardizedMq.getBranch(), standardizedMq);
                    }
                })
                .keyBy(0)
                .timeWindow(Time.minutes(Long.valueOf(PropertyUtil.getInstance().getProperty("flink.windowMinutes"))))
                     .aggregate(new RecPvAggregation(), new TypeWindowResultFunction(target));
    }

    /**
     * 推荐位级别
     * @param singleOutputStreamFilterOperator
     */
    public  DataStream<RecChartData> recCount(SingleOutputStreamOperator<StandardizedMq> singleOutputStreamFilterOperator, String target){
        //统计分支流量实时pv
        return singleOutputStreamFilterOperator
                .map(new MapFunction<StandardizedMq, Tuple2<String, StandardizedMq>>() {
                    @Override
                    public Tuple2<String, StandardizedMq> map(StandardizedMq standardizedMq) throws Exception {
                        // 统计推荐位实时数据时，只传推荐位编号，实验ID和规则集编码都写成-1
                        return new Tuple2<String, StandardizedMq>(standardizedMq.getRecloc(), standardizedMq);
                    }
                })
                .keyBy(0)
                .timeWindow(Time.minutes(Long.valueOf(PropertyUtil.getInstance().getProperty("flink.windowMinutes"))))
                .aggregate(new RecPvAggregation(), new RecLocWindowResultFunction(target));

    }
    /**
     * 试验位级别
     * @param singleOutputStreamFilterOperator
     */
    public  DataStream<RecChartData> expCount(SingleOutputStreamOperator<StandardizedMq> singleOutputStreamFilterOperator, String target){
        //统计分支流量实时pv
       return singleOutputStreamFilterOperator
                .map(new MapFunction<StandardizedMq, Tuple2<String, StandardizedMq>>() {
                    @Override
                    public Tuple2<String, StandardizedMq> map(StandardizedMq standardizedMq) throws Exception {
                        // 统计推荐位实时数据时，只传推荐位编号，实验ID和规则集编码都写成-1
                        String key=String.format("%s_%s",standardizedMq.getRecloc(),standardizedMq.getExperimentid());
                        return new Tuple2<String, StandardizedMq>(key, standardizedMq);
                    }
                })
                .keyBy(0)
                .timeWindow(Time.minutes(Long.valueOf(PropertyUtil.getInstance().getProperty("flink.windowMinutes"))))
               .aggregate(new RecPvAggregation(), new ExpWindowResultFunction(target));
    }

    /**
     * 规则集级别
     * @param singleOutputStreamFilterOperator
     */
    public  DataStream<RecChartData> ruleCount(SingleOutputStreamOperator<StandardizedMq> singleOutputStreamFilterOperator, String target){
        //统计分支流量实时pv
        return singleOutputStreamFilterOperator
                .map(new MapFunction<StandardizedMq, Tuple2<String, StandardizedMq>>() {
                    @Override
                    public Tuple2<String, StandardizedMq> map(StandardizedMq standardizedMq) throws Exception {
                        // 统计推荐位实时数据时，只传推荐位编号，实验ID和规则集编码都写成-1
                        String key=String.format("%s_%s_%s",standardizedMq.getRecloc(),standardizedMq.getExperimentid(),standardizedMq.getRule());
                        return new Tuple2<String, StandardizedMq>(key, standardizedMq);
                    }
                })
                .keyBy(0)
                .timeWindow(Time.minutes(Long.valueOf(PropertyUtil.getInstance().getProperty("flink.windowMinutes"))))
                .aggregate(new RecPvAggregation(), new RuleWindowResultFunction(target));
    }
}
