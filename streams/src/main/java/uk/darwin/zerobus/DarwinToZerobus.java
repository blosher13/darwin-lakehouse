package uk.darwin.zerobus;

import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Streams app: reads Darwin Push Port records written by the ActiveMQ source connector,
 * decompresses the XML and lands one row per message in a Databricks Delta table via Zerobus.
 *
 * Configuration comes from environment variables, secrets from the .ENV properties file.
 */
public class DarwinToZerobus {

    private static final Logger log = LoggerFactory.getLogger(DarwinToZerobus.class);

    public static void main(String[] args) throws Exception {
        Properties secrets = new Properties();
        try (FileReader reader = new FileReader(env("DARWIN_ENV_FILE", "/etc/darwin/.env"))) {
            secrets.load(reader);
        }

        String inputTopic = env("INPUT_TOPIC", "darwin-pushport-v16");
        String table = env("DATABRICKS_TABLE", optional(secrets, "databricks_catalog", "workspace") + "."
                + optional(secrets, "databricks_schema", "darwin") + ".pushport_bronze");

        ZerobusSink sink = new ZerobusSink(
                env("ZEROBUS_ENDPOINT", required(secrets, "zerobus_endpoint")),
                workspaceUrl(required(secrets, "databricks_host")),
                table,
                required(secrets, "databricks_client_id"),
                required(secrets, "databricks_client_secret"));

        Duration flushInterval = Duration.ofMillis(Long.parseLong(env("FLUSH_INTERVAL_MS", "5000")));

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
               .process(() -> new DarwinRowProcessor(sink, flushInterval));

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, env("APPLICATION_ID", "darwin-zerobus"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, env("BOOTSTRAP_SERVERS", "localhost:9092"));
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        // Commits are requested by DarwinRowProcessor after each Zerobus flush; push the
        // automatic interval commit out of the way so it can't commit un-acknowledged rows.
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Integer.MAX_VALUE);
        props.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        CountDownLatch stopped = new CountDownLatch(1);
        streams.setUncaughtExceptionHandler(e -> {
            log.error("Stream thread failed, shutting down", e);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        streams.setStateListener((now, before) -> {
            log.info("Kafka Streams state {} -> {}", before, now);
            if (now == KafkaStreams.State.NOT_RUNNING || now == KafkaStreams.State.ERROR) {
                stopped.countDown();
            }
        });
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        log.info("Streaming {} -> {} via Zerobus", inputTopic, table);
        streams.start();
        stopped.await();
        sink.close();
        // Non-zero exit so the container restart policy brings us back after a failure
        System.exit(streams.state() == KafkaStreams.State.ERROR ? 1 : 0);
    }

    /** "https://dbc-x.cloud.databricks.com/settings/...?o=123" -> "https://dbc-x.cloud.databricks.com" */
    static String workspaceUrl(String host) {
        java.net.URI uri = java.net.URI.create(host.trim());
        return uri.getScheme() + "://" + uri.getHost();
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            if (fallback == null) {
                throw new IllegalStateException("Environment variable " + name + " is required");
            }
            return fallback;
        }
        return value;
    }

    private static String optional(Properties props, String key, String fallback) {
        String value = props.getProperty(key);
        return value == null || value.isBlank() ? fallback : value.trim();
    }

    private static String required(Properties props, String key) throws IOException {
        String value = props.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IOException("Missing '" + key + "' in secrets file");
        }
        return value.trim();
    }
}
