package org.kafka.connect.source.fixer;


import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of the Task Interface for pulling the data from API
 * @author tahir ali
 */
public class ForeignExchangeRatesTask extends SourceTask {
    private static final String API_ENDPOINT = "url";
    private static final String TOPIC_NAME = "topic";
    private static final String SERVICE_FIELD = "service";
    private static final String POSITION_FIELD = "position";
    private static final String OFFSET_KEY = "fixer";
    private static final int SLEEP_TIME = 1;

    private String topic;
    private String url;
    private String latestDate;
    final AtomicBoolean done = new AtomicBoolean(false);
    FixerClient fixerClient;

    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;

    public String version() {
        return new ForeignExchangeRatesConnector().version();
    }

    /**
     * Initialization of the task, takes care of the configuration
     * plus creates the structured needed for communicating
     * with the client thread.
     */
    @Override
    public void start(final Map<String, String> map) {
        topic = map.get(TOPIC_NAME);
        url = map.get(API_ENDPOINT);
        final WebTarget webTarget = createClient(url);
        fixerClient = new FixerClient(webTarget);
    }

    /**
     * Polls the task for new data.
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        final ArrayList<SourceRecord> records = new ArrayList<>();
        TimeUnit.MINUTES.sleep(SLEEP_TIME);
        records.add(new SourceRecord(
                offsetKey(OFFSET_KEY),
                offsetValue(System.nanoTime()), topic,
                VALUE_SCHEMA, fixerClient.executeRequest()));
        return records;

    }

    @Override
    public void stop() {}

    private Map<String, String> offsetKey(String service) {
        return Collections.singletonMap(SERVICE_FIELD, service);
    }

    private Map<String, Long> offsetValue(Long pos) {
        return Collections.singletonMap(POSITION_FIELD, pos);
    }

    private WebTarget createClient(String uri) {
        final ClientBuilder builder = ClientBuilder.newBuilder();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.property(ClientProperties.CONNECT_TIMEOUT, 20_000);
        clientConfig.property(ClientProperties.READ_TIMEOUT, 30_000);
        return builder
                .withConfig(clientConfig)
                .build()
                .target(uri);
    }
}
