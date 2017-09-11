package com.smartertravel.kafka.demo;

import jdk.net.Sockets;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.Collection;
import java.util.Map;

/**
 *
 *
 */
public class NetworkHangSinkTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(NetworkHangSinkTask.class);
    private static final int WRITE_RETRIES = 5;

    private final Object lock = new Object();
    private NetworkHangConfig config;
    private Socket socket;

    @Override
    public String version() {
        return null;
    }

    @Override
    public void start(Map<String, String> config) {
        final NetworkHangConfig parsed = new NetworkHangConfig(NetworkHangConfig.CONFIG_DEF, config);
        final Socket socket = new Socket();

        synchronized (lock) {
            this.config = parsed;
            this.socket = socket;
        }

        connectSocket();
    }

    private static void close(Socket sock) {
        if (sock != null) {
            try {
                sock.close();
            } catch (IOException e) {
                LOGGER.warn("Error closing socket", e);
            }
        }
    }

    private void connectSocket() {
        synchronized (lock) {
            close(this.socket);
            this.socket = new Socket();

            try {
                socket.connect(new InetSocketAddress(config.getString(NetworkHangConfig.SERVER_HOST), config.getInt(NetworkHangConfig.SERVER_PORT)));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        byte[] someBytes = new byte[1024];

        try {
            for (int i = 0; i < this.config.getInt(NetworkHangConfig.NUM_WRITES); i++) {
                writeOrReconnect(someBytes);
                if (i % 10 == 0) {
                    LOGGER.info("Wrote batch {} of 1K bytes", i);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeOrReconnect(byte[] bytes) throws IOException {
        for (int i = 0; i < WRITE_RETRIES; i++) {
            try {
                final OutputStream stream = socket.getOutputStream();
                stream.write(bytes);
                return;
            } catch (SocketException e) {
                LOGGER.info("Reconnecting socket after write error: {}", e.getMessage());
                connectSocket();
            }
        }
    }

    @Override
    public void stop() {
        synchronized (lock) {
            if (this.socket != null) {
                try {
                    this.socket.close();
                } catch (IOException e) {
                    LOGGER.warn("Failed to close socket", e);
                }
            } else {
                LOGGER.info("Null socket during shutdown, weird");
            }
        }
    }
}
