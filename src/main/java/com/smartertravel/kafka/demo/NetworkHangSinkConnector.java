package com.smartertravel.kafka.demo;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 *
 */
public class NetworkHangSinkConnector extends SinkConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(NetworkHangSinkConnector.class);

    private final Object lock = new Object();
    private Map<String, String> taskConfigs;
    private ExecutorService executor;
    private ServerSocket server;

    @Override
    public String version() {
        return "demo";
    }

    @Override
    public void start(Map<String, String> config) {
        LOGGER.info("Starting NetworkHangSinkConnector connector");

        final AtomicInteger i = new AtomicInteger(1);
        final NetworkHangConfig parsed = new NetworkHangConfig(NetworkHangConfig.CONFIG_DEF, config);
        final ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NetworkHang-thread-" + i.getAndIncrement());
            }
        });

        final ServerSocket server;
        try {
            server = new ServerSocket(parsed.getInt(NetworkHangConfig.SERVER_PORT));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        final CountDownLatch latch = new CountDownLatch(1);

        executor.submit(new Runnable() {
            @Override
            public void run() {
                while (!server.isClosed()) {
                    final Socket socket;

                    try {
                        LOGGER.info("Starting server that will never read incoming data...");
                        latch.countDown();
                        socket = server.accept();
                        LOGGER.info("Accepted connection {}", socket.toString());
                    } catch (IOException e) {
                        LOGGER.error("Accept error", e);
                    }
                }
            }
        });

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted trying to wait for server to start");
        }

        synchronized (lock) {
            this.executor = executor;
            this.server = server;
            this.taskConfigs = Collections.unmodifiableMap(new HashMap<>(config));
        }
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

        if (this.server != null) {
            try {
                this.server.close();
                LOGGER.info("Stopped ServerSocket");
            } catch (IOException e) {
                LOGGER.warn("Failed to stop ServerSocket", e);
            }
        } else {
            LOGGER.info("Null server during shutdown, weird");
        }
    }

    @Override
    public ConfigDef config() {
        return NetworkHangConfig.CONFIG_DEF;
    }
}
