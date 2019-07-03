package com.jd.jr.recommender.mq.source;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FakeSource extends RichParallelSourceFunction<String> {

    private final Logger logger = LoggerFactory.getLogger(FakeSource.class);

    private boolean isRunning = true;
    private int count;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (isRunning) {
            count++;
            if (count % 100 == 0) {
                System.out.println("JSON count is: " + count);
            }

            double rand = Math.random();
            String json = "";
            if (rand < 0.2) {
                json = "{\"uid\":\"aaa\",\"pin\":\"pin1\",\"recloc\":\"100045\",\"rule\":\"rule1\",\"experimentid\":\"2\",\"id\":\"111\",\"type\":\"imp\",\"branch\":\"1\",\"timestamp\":\"1561948695735\"}";
            } else if (rand < 0.4) {
                json = "{\"uid\":\"bbb\",\"pin\":\"pin2\",\"recloc\":\"100057\",\"rule\":\"rule2\",\"experimentid\":\"2\",\"id\":\"111\",\"type\":\"imp\",\"branch\":\"2\",\"timestamp\":\"1561948695735\"}";
            } else if (rand < 0.6) {
                json = "{\"uid\":\"ccc\",\"pin\":\"pin3\",\"recloc\":\"100045\",\"rule\":\"rule3\",\"experimentid\":\"2\",\"id\":\"111\",\"type\":\"imp\",\"branch\":\"1\",\"timestamp\":\"1561948695735\"}";
            } else if (rand < 0.8) {
                json = "{\"uid\":\"ddd\",\"pin\":\"pin4\",\"recloc\":\"100049\",\"rule\":\"rule4\",\"experimentid\":\"2\",\"id\":\"111\",\"type\":\"imp\",\"branch\":\"1\",\"timestamp\":\"1561948695735\"}";
            } else {
                json = "{\"uid\":\"eee\",\"pin\":\"pin5\",\"recloc\":\"100045\",\"rule\":\"rule1\",\"experimentid\":\"2\",\"id\":\"111\",\"type\":\"imp\",\"branch\":\"1\",\"timestamp\":\"1561948695735\"}";
            }

            for (int i = 0; i < 10; i++) {
                ctx.collect(json);
            }

//            logger.debug("Generated a new message: " + json);

//            long sleepms = 1000;
            long sleepms = Math.round(Math.random() * 1000);
            Thread.sleep(sleepms);
        }
        System.out.println("==========STOP================");
    }

    @Override
    public void cancel() {
        System.out.println("==========CANCEL================");
        isRunning = false;
    }
}
