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
public class NetworkHangSinkConnector extends SinkConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(NetworkHangSinkConnector.class);

    private Map<String, String> taskConfigs;

    @Override
    public String version() {
        return "demo";
    }

    @Override
    public void start(Map<String, String> config) {
        LOGGER.info("Starting NetworkHangSinkConnector connector");
        this.taskConfigs = Collections.unmodifiableMap(new HashMap<>(config));
    }

    @Override
    public Class<? extends Task> taskClass() {
        return NetworkHangSinkTask.class;
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
        LOGGER.info("Stopping NetworkHangSinkConnector connector");
    }

    @Override
    public ConfigDef config() {
        return NetworkHangConfig.CONFIG_DEF;
    }
}
