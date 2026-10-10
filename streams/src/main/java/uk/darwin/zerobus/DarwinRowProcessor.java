package uk.darwin.zerobus;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import com.databricks.zerobus.ZerobusException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;

/**
 * Turns one ActiveMQ-connector record (JSON with JMS metadata and a base64 gzipped body)
 * into a bronze row and hands it to Zerobus.
 *
 * Offsets are committed only from {@link #flushAndCommit}, after Zerobus has acknowledged every
 * queued row, so a crash re-delivers rather than loses messages (at-least-once). The app turns
 * off Kafka Streams' own interval commits for this to hold.
 */
public class DarwinRowProcessor implements Processor<String, String, Void, Void> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ZerobusSink sink;
    private final Duration flushInterval;
    private ProcessorContext<Void, Void> context;

    public DarwinRowProcessor(ZerobusSink sink, Duration flushInterval) {
        this.sink = sink;
        this.flushInterval = flushInterval;
    }

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        this.context = context;
        context.schedule(flushInterval, PunctuationType.WALL_CLOCK_TIME, now -> flushAndCommit());
    }

    @Override
    public void process(Record<String, String> record) {
        RecordMetadata meta = context.recordMetadata().orElseThrow();
        try {
            JsonNode in = MAPPER.readTree(record.value());
            JsonNode properties = in.path("properties");

            ObjectNode row = MAPPER.createObjectNode();
            row.put("kafka_partition", meta.partition());
            row.put("kafka_offset", meta.offset());
            row.put("message_id", in.path("messageID").asText(null));
            putLong(row, "sequence_number", jmsProperty(properties, "SequenceNumber"));
            row.put("message_type", textOrNull(jmsProperty(properties, "MessageType")));
            // Delta TIMESTAMP columns are sent as microseconds since the epoch
            row.put("published_timestamp", TimeUnit.MILLISECONDS.toMicros(in.path("timestamp").asLong()));
            row.put("ingested_at", TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
            row.put("message_xml", gunzip(Base64.getDecoder().decode(in.path("bytes").asText())));

            sink.ingest(row.toString());
        } catch (IOException e) {
            throw new StreamsException("Could not decode record at offset " + meta.offset(), e);
        } catch (ZerobusException e) {
            throw new StreamsException("Zerobus rejected record at offset " + meta.offset(), e);
        }
    }

    @Override
    public void close() {
        flushAndCommit();
    }

    private void flushAndCommit() {
        try {
            sink.flush();
        } catch (ZerobusException e) {
            // Fails the task before the commit, so un-acknowledged rows are re-read on restart
            throw new StreamsException("Zerobus flush failed", e);
        }
        context.commit();
    }

    /** The connector stores each JMS property as {"propertyType": "integer", "integer": 42, "string": null, ...}. */
    private static JsonNode jmsProperty(JsonNode properties, String name) {
        JsonNode property = properties.path(name);
        String type = property.path("propertyType").asText();
        return type.isEmpty() ? null : property.get(type);
    }

    private static void putLong(ObjectNode row, String field, JsonNode value) {
        if (value == null || value.isNull()) {
            row.putNull(field);
        } else {
            row.put(field, value.isNumber() ? value.asLong() : Long.parseLong(value.asText()));
        }
    }

    private static String textOrNull(JsonNode value) {
        return value == null || value.isNull() ? null : value.asText();
    }

    private static String gunzip(byte[] compressed) throws IOException {
        try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(compressed))) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
