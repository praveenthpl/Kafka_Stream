
package io.amient.kafka.connect.embedded;

import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.storage.*;
import org.apache.kafka.connect.util.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;



public class ConnectEmbedded {
    private static final Logger log = LoggerFactory.getLogger(ConnectEmbedded.class);
    private static final int REQUEST_TIMEOUT_MS = 120000;

    private final Worker worker;
    private final DistributedHerder herder;
    private final CountDownLatch startLatch = new CountDownLatch(1);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final CountDownLatch stopLatch = new CountDownLatch(1);

    private final ShutdownHook shutdownHook;
    private final Properties[] connectorConfigs;

    public ConnectEmbedded(Properties workerConfig, Properties... connectorConfigs) throws Exception {
        Time time = new SystemTime();
        DistributedConfig config = new DistributedConfig(Utils.propsToStringMap(workerConfig));

        KafkaOffsetBackingStore offsetBackingStore = new KafkaOffsetBackingStore();
        offsetBackingStore.configure(config);

         String workerId = UUID.randomUUID().toString();
        worker = new Worker(workerId, time, config, offsetBackingStore);

        StatusBackingStore statusBackingStore = new KafkaStatusBackingStore(time, worker.getInternalValueConverter());
        statusBackingStore.configure(config);

        ConfigBackingStore configBackingStore = new KafkaConfigBackingStore(worker.getInternalValueConverter());
        configBackingStore.configure(config);

       herder = new DistributedHerder(config, time, worker, statusBackingStore, configBackingStore, "");
        this.connectorConfigs = connectorConfigs;

        shutdownHook = new ShutdownHook();
    }

    public void start() {
        try {
            log.info("Kafka ConnectEmbedded starting");
            Runtime.getRuntime().addShutdownHook(shutdownHook);

            worker.start();
            herder.start();

            log.info("Kafka ConnectEmbedded started");

            for (Properties connectorConfig : connectorConfigs) {
                FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>();
                String name = connectorConfig.getProperty(ConnectorConfig.NAME_CONFIG);
                herder.putConnectorConfig(name, Utils.propsToStringMap(connectorConfig), true, cb);
                cb.get(REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            }

        } catch (InterruptedException e) {
            log.error("Starting interrupted ", e);
        } catch (ExecutionException e) {
            log.error("Submitting connector config failed", e.getCause());
        } catch (TimeoutException e) {
            log.error("Submitting connector config timed out", e);
        } finally {
            startLatch.countDown();
        }
    }

    public void stop() {
        try {
            boolean wasShuttingDown = shutdown.getAndSet(true);
            if (!wasShuttingDown) {

                log.info("Kafka ConnectEmbedded stopping");
                herder.stop();
                worker.stop();

                log.info("Kafka ConnectEmbedded stopped");
            }
        } finally {
            stopLatch.countDown();
        }
    }

    public void awaitStop() {
        try {
            stopLatch.await();
        } catch (InterruptedException e) {
            log.error("Interrupted waiting for Kafka Connect to shutdown");
        }
    }

    private class ShutdownHook extends Thread {
        @Override
        public void run() {
            try {
                startLatch.await();
                ConnectEmbedded.this.stop();
            } catch (InterruptedException e) {
                log.error("Interrupted in shutdown hook while waiting for Kafka Connect startup to finish");
            }
        }
    }
}
