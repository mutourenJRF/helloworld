package com.jd.jr.recommender.mq.source;

import com.jd.jmq.client.consumer.MessageListener;
import com.jd.jmq.common.message.Message;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ExposureMessageListener implements MessageListener {

    private final Logger logger = LoggerFactory.getLogger(ExposureMessageListener.class);

    private int count;
    private SourceFunction.SourceContext<String> ctx;

    public ExposureMessageListener(SourceFunction.SourceContext<String> ctx) {
        this.ctx = ctx;
    }

    @Override
    public void onMessage(List<Message> messages) throws Exception {
        if (messages != null && !messages.isEmpty()) {
            for (Message message : messages) {
                count++;
                if (count % 50 == 0) {
                    System.out.println("Received " + count + " messages");
                }

//                logger.debug("Got a exposure message: " + message.getText());
                ctx.collect(message.getText());
            }
        }
    }
}
