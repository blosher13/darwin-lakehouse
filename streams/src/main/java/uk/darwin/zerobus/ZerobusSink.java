package uk.darwin.zerobus;

import com.databricks.zerobus.ZerobusException;
import com.databricks.zerobus.ZerobusJsonStream;
import com.databricks.zerobus.ZerobusSdk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Thin wrapper around a single Zerobus JSON stream into one Delta table. */
public class ZerobusSink implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ZerobusSink.class);

    private final ZerobusSdk sdk;
    private final ZerobusJsonStream stream;
    private long sinceLastFlush;

    public ZerobusSink(String endpoint, String workspaceUrl, String table, String clientId, String clientSecret) {
        this.sdk = new ZerobusSdk(endpoint, workspaceUrl);
        this.stream = sdk.streamBuilder()
                .table(table)
                .oauth(clientId, clientSecret)
                .recovery(true)
                .json()
                .build()
                .join();
        log.info("Opened Zerobus stream to {}", table);
    }

    /** Queues a JSON row; durability is only guaranteed after {@link #flush()}. */
    public void ingest(String jsonRow) throws ZerobusException {
        stream.ingestRecordOffset(jsonRow);
        sinceLastFlush++;
    }

    /** Blocks until every queued row has been acknowledged by Zerobus. */
    public void flush() throws ZerobusException {
        if (sinceLastFlush == 0) {
            return;
        }
        stream.flush();
        log.info("Flushed {} rows to Zerobus", sinceLastFlush);
        sinceLastFlush = 0;
    }

    @Override
    public void close() {
        try {
            stream.close();
        } catch (ZerobusException e) {
            log.warn("Error closing Zerobus stream", e);
        }
        sdk.close();
    }
}
