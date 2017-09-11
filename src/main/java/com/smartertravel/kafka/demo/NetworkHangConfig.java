package com.smartertravel.kafka.demo;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

/**
 *
 *
 */
public class NetworkHangConfig extends AbstractConfig {

    public static final String SERVER_HOST = "server.host";
    public static final String SERVER_PORT = "server.port";
    public static final String NUM_WRITES = "num.writes";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(SERVER_HOST, Type.STRING, "localhost", null, Importance.HIGH, "Host to write to that should hang")
            .define(SERVER_PORT, Type.INT, 12341, null, Importance.HIGH, "Port to write to that should hang")
            .define(NUM_WRITES, Type.INT, 8192, ConfigDef.Range.atLeast(0), Importance.MEDIUM, "Number of 1kb writes that will be attempted");

    public static ConfigDef getConfigDef() {
        return CONFIG_DEF;
    }

    public NetworkHangConfig(ConfigDef definition, Map<?, ?> originals, boolean doLog) {
        super(definition, originals, doLog);
    }

    public NetworkHangConfig(ConfigDef definition, Map<?, ?> originals) {
        super(definition, originals);
    }

    public NetworkHangConfig(Map<String, Object> parsedConfig) {
        super(parsedConfig);
    }
}
