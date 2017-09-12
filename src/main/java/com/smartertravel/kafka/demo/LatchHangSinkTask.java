package com.smartertravel.kafka.demo;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Sink task that will block on a {@link CountDownLatch} in its {@link #put(Collection)}
 * method and never unblock (unless the thread is interrupted).
 * <p>
 * This task behaves this way to demonstrate how Kafka Connect workers can be put into
 * an invalid state by buggy or badly behaved Connectors / Tasks.
 */
public class LatchHangSinkTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(LatchHangSinkTask.class);
    private CountDownLatch latch;

    @Override
    public String version() {
        return "1.0.0-demo-task";
    }

    @Override
    public void start(Map<String, String> config) {
        LOGGER.info("Setting up a countdown latch to block forever in {}", Thread.currentThread().getName());
        this.latch = new CountDownLatch(1);
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        final Thread t = Thread.currentThread();
        LOGGER.info("Starting to block on countdown latch in {}", t.getName());

        try {
            this.latch.await();
        } catch (InterruptedException e) {
            LOGGER.info("Interrupted while waiting for countdown latch: {}", e.getMessage());
        }

        LOGGER.info("Finished waiting for countdown latch in {}", t.getName());
    }

    @Override
    public void stop() {
        LOGGER.info(""
                + "Stop method for LatchHangSinkTask called from {} but we won't stop the "
                + "latch because we are evil", Thread.currentThread().getName());
    }
}
