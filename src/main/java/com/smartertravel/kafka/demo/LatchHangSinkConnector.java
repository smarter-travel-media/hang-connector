package com.smartertravel.kafka.demo;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 *
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
