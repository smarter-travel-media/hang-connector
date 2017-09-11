package com.smartertravel.kafka.demo;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 *
 */
public class NetworkHangSinkTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(NetworkHangSinkTask.class);
    private final Object lock = new Object();
    private NetworkHangConfig config;
    private ExecutorService executor;
    private ServerSocket server;
    private Socket socket;

    @Override
    public String version() {
        return null;
    }

    @Override
    public void start(Map<String, String> config) {
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

        executor.submit(new Runnable() {
            @Override
            public void run() {
                while (!server.isClosed()) {
                    final Socket socket;

                    try {
                        socket = server.accept();
                        LOGGER.info("Accepted connection {}", socket.toString());
                    } catch (IOException e) {
                        LOGGER.error("Accept error", e);
                    }
                }
            }
        });

        final Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(parsed.getString(NetworkHangConfig.SERVER_HOST), parsed.getInt(NetworkHangConfig.SERVER_PORT)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        synchronized (lock) {
            this.config = parsed;
            this.executor = executor;
            this.server = server;
            this.socket = socket;
        }
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        byte[] someBytes = new byte[1024];

        try {
            final OutputStream stream = socket.getOutputStream();
            for (int i = 0; i < this.config.getInt(NetworkHangConfig.NUM_WRITES); i++) {
                stream.write(someBytes);
                LOGGER.info("Wrote batch %s of 1K bytes%n", i);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        if (this.socket != null) {
            try {
                this.socket.close();
            } catch (IOException e) {
                LOGGER.warn("Failed to close socket", e);
            }
        } else {
            LOGGER.info("Null socket during shutdown, weird");
        }

        if (this.server != null) {
            try {
                this.server.close();
            } catch (IOException e) {
                LOGGER.warn("Failed to stop ServerSocket", e);
            }
        } else {
            LOGGER.info("Null server during shutdown, weird");
        }

        if (this.executor != null) {
            this.executor.shutdown();
        } else {
            LOGGER.info("Null executor during shutdown, weird");
        }

    }
}
