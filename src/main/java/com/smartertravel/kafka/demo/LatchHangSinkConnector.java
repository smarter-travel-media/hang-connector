package com.smartertravel.kafka.demo;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Sink connector that spawns tasks that will block when their {@link SinkTask#put(Collection)}
 * method is called until they are forcibly interrupted.
 * <p>
 * This connector and its tasks behave this way to demonstrate how Kafka Connect workers can be
 * put into an invalid state by buggy or badly behaved Connectors / Tasks.
 */
public class LatchHangSinkConnector extends SinkConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(LatchHangSinkConnector.class);

    private Map<String, String> taskConfigs;

    @Override
    public String version() {
        return "1.0.0-demo-connector";
    }

    @Override
    public void start(Map<String, String> config) {
        LOGGER.info("Starting LatchHangSinkConnector connector in {}", Thread.currentThread().getName());
        this.taskConfigs = Collections.unmodifiableMap(new HashMap<>(config));
    }

    @Override
    public Class<? extends Task> taskClass() {
        return LatchHangSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            configs.add(taskConfigs);
        }

        return configs;
    }

    @Override
    public void stop() {
        LOGGER.info("Stopping LatchHangSinkConnector connector in {}", Thread.currentThread().getName());
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }
}
