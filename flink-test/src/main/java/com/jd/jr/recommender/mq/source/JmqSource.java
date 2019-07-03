package com.jd.jr.recommender.mq.source;

import com.jd.jmq.client.connection.ClusterTransportManager;
import com.jd.jmq.client.connection.TransportConfig;
import com.jd.jmq.client.connection.TransportManager;
import com.jd.jmq.client.consumer.ConsumerConfig;
import com.jd.jmq.client.consumer.MessageConsumer;
import com.jd.jr.recommender.mq.bean.JmqProperties;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class JmqSource extends RichParallelSourceFunction<String> {

    private boolean isRunning = true;
    private JmqProperties jmqProperties;
    private TransportManager manager;
    private MessageConsumer consumer;

    public JmqSource(JmqProperties jmqProperties) {
        this.jmqProperties = jmqProperties;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        TransportConfig config = new TransportConfig();
        config.setApp(this.jmqProperties.getApp());
        //设置broker地址
        config.setAddress(this.jmqProperties.getAddress());
        //设置用户名
        config.setUser(this.jmqProperties.getUser());
        //设置密码
        config.setPassword(this.jmqProperties.getPassword());
        //设置发送超时
        config.setSendTimeout(this.jmqProperties.getSendTimeout());
        //设置是否使用epoll模式，windows环境下设置为false，linux环境下设置为true
        config.setEpoll(this.jmqProperties.getEpoll());

        ConsumerConfig consumerConfig = new ConsumerConfig();
        manager = new ClusterTransportManager(config);
        consumer = new MessageConsumer(consumerConfig, manager, null);
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        ExposureMessageListener listener = new ExposureMessageListener(ctx);

        consumer.start();
        consumer.subscribe(this.jmqProperties.getStandardizedTrackingTopic(), listener);

        while (isRunning) {
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        if (consumer != null && !consumer.isStopped()) {
            consumer.stop();
        }
        if (manager != null && manager.isStarted()) {
            manager.stop();
        }
        System.out.println("==========CANCEL================");
    }
}
